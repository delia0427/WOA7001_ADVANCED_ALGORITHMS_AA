"""
Sketching + LSH Pipeline (Count-Min, HyperLogLog, AMS variants)

Purpose
- Build a randomized, scalable anomaly detection pipeline using Sketching (CMS/HLL/AMS) and Locality-Sensitive Hashing (LSH).
- Flow per record: Impute → bucketize → update CMS/HLL → insert into LSH → update AMS-F2 (second moment) sketch.
- At window end: compute per-region stats from sketches, including p̂ via CMS, Wilson CI, heavy buckets, HLL distinct count, AMS-F2, and LSH big-bucket summaries.
- Export stats via reporting sinks (JSONL/CSV/Stdout/Webhook). Alerts are optional and can be added separately.

Notes
- This file focuses on Sketching and LSH; reservoir/bootstrap and alert logic are intentionally omitted.
- AMS variant implemented here is a streaming AMS-F2 sketch with multiple replicates to reduce variance.

Config keys (recommended)
- regions: ["R2","R3","R4"]
- bucket_width: 5.0
- count_min: { epsilon: 0.001, delta: 0.01, conservative_update: true, max_bucket: 40 }
- hyperloglog: { precision_m: 16 }
- lsh: { dim: 4, num_bits: 16, num_tables: 4, seed: 42 }
- thresholds: { anomaly_reading: 145.0, min_confidence_for_hll: 0.60 }
- estimation: { heavy_ratio_threshold: 0.05, alpha: 0.05 }
- reporting.sinks: see modules/reporting/sink.py docstring
"""

from dataclasses import dataclass
from typing import Iterable, Dict, Any, List, Tuple, Optional
import math
import time

from .types import SensorRecord, ImputedRecord
from src.modules.imputation.imputer import Imputer
from src.modules.sketches.count_min import CountMinSketch
from src.modules.sketches.hll import HyperLogLog
from src.modules.lsh.lsh import SRPLSH
from src.modules.reporting.sink import build_sinks_from_config, ReportSink


# ------------------------------ AMS-F2 Sketch (Variant) ------------------------------ #
class AMSF2Sketch:
    """
    Simple AMS second moment (F2) sketch with R replicates:
    - Maintain per-region replicate sums S_j = sum_{events} s_j(key), where s_j(key) ∈ {+1, -1}
      is a stable random sign hash per replicate.
    - Estimate F2 as mean_j (S_j^2). Multiple replicates reduce variance.
    """

    def __init__(self, num_replicates: int = 5, seed: int = 1337):
        self.R = int(num_replicates)
        self.seed = int(seed)
        # region -> [S_0, S_1, ..., S_{R-1}]
        self._sums_by_region: Dict[str, List[int]] = {}

    def _sign_hash(self, key: Any, r: int) -> int:
        # Simple stable hash to ±1 using Python's hash with a replicate-specific salt
        h = hash((key, self.seed + r))
        return 1 if (h & 1) == 0 else -1

    def add(self, region: str, key: Any) -> None:
        sums = self._sums_by_region.setdefault(region, [0] * self.R)
        for r in range(self.R):
            sums[r] += self._sign_hash(key, r)

    def estimate_f2(self, region: str) -> float:
        sums = self._sums_by_region.get(region, None)
        if not sums:
            return 0.0
        return sum(float(s * s) for s in sums) / float(len(sums))


# ------------------------------ Window Stats (Sketch + LSH) ------------------------------ #
@dataclass
class SketchLSHWindowStats:
    region: str
    p_hat: float                 # anomaly proportion from CMS (abnormal/total)
    ci_low: float                # Wilson CI lower
    ci_high: float               # Wilson CI upper
    cms_total: int               # total CMS counts per region
    cms_abnormal: int            # abnormal CMS counts per region
    heavy_buckets: List[int]     # bucket indices considered "heavy"
    heavy_bucket_ratio: float    # heavy bucket share among all buckets
    hll_estimate: int            # distinct anomalous devices
    ams_f2: float                # AMS-F2 estimate (second moment)
    lsh_big_buckets: List[Dict[str, Any]]  # top-K large LSH buckets (table_idx, signature, size, means)


# ------------------------------ Pipeline (Sketching + LSH) ------------------------------ #
class Pipeline:
    def __init__(self, config: Dict[str, Any]):
        self.cfg = config or {}

        # Imputer (basic)
        self.imputer = Imputer(self.cfg)

        # Count-Min Sketch
        cm = self.cfg.get("count_min", {}) or {}
        self.cms = CountMinSketch(
            epsilon=float(cm.get("epsilon", 0.001)),
            delta=float(cm.get("delta", 0.01)),
            conservative=bool(cm.get("conservative_update", True)),
        )

        # HyperLogLog (variant)
        self.hll = HyperLogLog(precision_m=int(self.cfg.get("hyperloglog", {}).get("precision_m", 16)))

        # LSH (SRP)
        lsh_cfg = self.cfg.get("lsh", {}) or {}
        self.lsh = SRPLSH(
            dim=int(lsh_cfg.get("dim", 4)),
            num_bits=int(lsh_cfg.get("num_bits", 16)),
            num_tables=int(lsh_cfg.get("num_tables", 4)),
            seed=int(lsh_cfg.get("seed", 42)),
        )

        # AMS-F2 sketch (variant)
        ams_cfg = self.cfg.get("ams", {}) or {}
        self.ams = AMSF2Sketch(
            num_replicates=int(ams_cfg.get("num_replicates", 5)),
            seed=int(ams_cfg.get("seed", 1337)),
        )

        # Cached thresholds and params
        th = self.cfg.get("thresholds", {}) or {}
        self.anomaly_reading = float(th.get("anomaly_reading", 145.0))
        self.min_conf_for_hll = float(th.get("min_confidence_for_hll", 0.60))
        self.bucket_width = float(self.cfg.get("bucket_width", 5.0))
        cm_max_bucket_default = int(200 // max(1.0, self.bucket_width))
        self.max_bucket = int(cm.get("max_bucket", cm_max_bucket_default))
        est = self.cfg.get("estimation", {}) or {}
        self.alpha = float(est.get("alpha", 0.05))
        self.heavy_ratio_threshold = float(est.get("heavy_ratio_threshold", 0.05))

        # Regions (fall back to defaults)
        self.regions = list(self.cfg.get("regions", [])) or ["R2", "R3", "R4"]

        # Reporting sinks
        self.reporting_sinks: List[ReportSink] = build_sinks_from_config(self.cfg)

        # Internal counters
        self._window_count = 0

    # -------------------------- Stream processing -------------------------- #
    def process_stream(self, stream: Iterable[SensorRecord], window_id: str) -> List[SketchLSHWindowStats]:
        """
        Stream processing:
        - impute → bucketize
        - update CMS/HLL
        - insert features into LSH
        - update AMS-F2 per region
        Returns [] (stats are computed at window end via get_window_stats_sketch_lsh).
        """
        self._window_count = 0
        for r in stream:
            imp = self.imputer.impute(r)
            # Features for LSH (dim must match SRPLSH.dim)
            risk_multiplier = self._risk_multiplier(imp.region)
            imp.features = [float(imp.reading_hat), float(imp.confidence), float(risk_multiplier), 0.0]

            # CMS: (region, bucket)
            bucket = self._bucket(imp.reading_hat, width=self.bucket_width)
            key_bucket = (imp.region, bucket)
            self.cms.update(key_bucket, amount=1)

            # HLL: distinct anomalous device (high reading + confidence)
            if imp.reading_hat > self.anomaly_reading and imp.confidence >= self.min_conf_for_hll:
                self.hll.add(imp.sensor_id)

            # LSH: insert feature vector
            try:
                self.lsh.insert(imp.features, imp.sensor_id)
            except Exception:
                # Dimension mismatch or other issues shouldn't block streaming
                pass

            # AMS-F2: update region replicate sums using bucket key
            self.ams.add(imp.region, key_bucket)

            self._window_count += 1

        return []

    # -------------------------- Window stats (Sketch + LSH) -------------------------- #
    def get_window_stats_sketch_lsh(self) -> List[SketchLSHWindowStats]:
        """
        Compute per-region window stats based on Sketching and LSH only:
        - anomaly proportion p̂ from CMS (abnormal/total)
        - Wilson CI on p̂ using CMS counts
        - heavy buckets (CMS fraction ≥ heavy_ratio_threshold)
        - HLL distinct anomalous count
        - AMS-F2 estimate
        - LSH big-bucket summaries
        """
        stats: List[SketchLSHWindowStats] = []

        # Precompute LSH big buckets once per window
        lsh_big_buckets = self.lsh.bucket_stats(top_k=10, min_size=3)

        for region in self.regions:
            cms_total = 0
            cms_abnormal = 0
            heavy_buckets: List[int] = []

            for b in range(self.max_bucket + 1):
                f = int(self.cms.query((region, b)))
                cms_total += f
                if (b * self.bucket_width) >= self.anomaly_reading:
                    cms_abnormal += f

            heavy_bucket_ratio = 0.0
            if cms_total > 0:
                for b in range(self.max_bucket + 1):
                    f = int(self.cms.query((region, b)))
                    if (f / float(cms_total)) >= self.heavy_ratio_threshold:
                        heavy_buckets.append(b)
                heavy_bucket_ratio = len(heavy_buckets) / float(max(1, (self.max_bucket + 1)))

            # p̂ and Wilson CI
            p_hat, (ci_low, ci_high) = self._wilson_interval(cms_abnormal, cms_total, alpha=self.alpha)

            # HLL distinct anomalies (global; optionally partition by region if needed)
            hll_estimate = int(self.hll.estimate())

            # AMS-F2 per region
            ams_f2 = float(self.ams.estimate_f2(region))

            stats.append(
                SketchLSHWindowStats(
                    region=region,
                    p_hat=p_hat,
                    ci_low=ci_low,
                    ci_high=ci_high,
                    cms_total=cms_total,
                    cms_abnormal=cms_abnormal,
                    heavy_buckets=heavy_buckets,
                    heavy_bucket_ratio=heavy_bucket_ratio,
                    hll_estimate=hll_estimate,
                    ams_f2=ams_f2,
                    lsh_big_buckets=lsh_big_buckets,
                )
            )

        return stats

    # -------------------------- Export (stats only) -------------------------- #
    def export_sketch_lsh(self, window_id: str) -> List[SketchLSHWindowStats]:
        """
        Compute Sketch+LSH window stats and write to reporting sinks.
        Returns stats list. Alerts are not part of this pipeline variant.
        """
        stats = self.get_window_stats_sketch_lsh()
        for sink in self.reporting_sinks:
            try:
                # Reuse sink write_window_stats API; sinks will serialize known fields.
                sink.write_window_stats(stats, window_id)
            except Exception as e:
                print(f"[Sketch+LSH] write_window_stats failed: {e}")
        return stats

    # -------------------------- Helpers -------------------------- #
    def _risk_multiplier(self, region: str) -> float:
        return self.cfg.get("risk_multipliers", {}).get(
            region, self.cfg.get("risk_multipliers", {}).get("default", 1.0)
        )

    def _bucket(self, reading_hat: float, width: float = 5.0) -> int:
        return int(float(reading_hat) // float(width))

    def _wilson_interval(self, k: int, n: int, alpha: Optional[float] = None) -> Tuple[float, Tuple[float, float]]:
        """
        Wilson score interval for binomial proportion (p̂ = k/n).
        """
        alpha = self.alpha if alpha is None else float(alpha)
        if n <= 0:
            return 0.0, (0.0, 0.0)
        z = 1.959964 if abs(alpha - 0.05) < 1e-9 else 1.644854 if abs(alpha - 0.10) < 1e-9 else 1.959964
        p = float(k) / float(n)
        denom = 1.0 + (z * z) / float(n)
        center = p + (z * z) / (2.0 * float(n))
        delta = z * ((p * (1.0 - p) / float(n)) + (z * z) / (4.0 * float(n * n))) ** 0.5
        low = max(0.0, (center - delta) / denom)
        high = min(1.0, (center + delta) / denom)
        return p, (low, high)