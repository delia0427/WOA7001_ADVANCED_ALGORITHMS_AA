"""
Sketching-only Pipeline (Count-Min + AMS + HyperLogLog)

新增：
- AMSF2Sketch：估计每区域二阶矩 F2，反映重尾/爆发强度
- 在窗口统计中输出 ams_f2 字段

配置示例（与默认兼容）：
- ams: { num_sketches: 64, seed: 0 }
- count_min: { epsilon: 0.001, delta: 0.01, conservative_update: true, max_bucket: 40 }
- hyperloglog: { precision_m: 16 }  # 当前为占位版集合计数
- bucket_width: 5.0
- thresholds: { anomaly_reading: 145.0, min_confidence_for_hll: 0.60 }
- estimation: { heavy_ratio_threshold: 0.05, alpha: 0.05 }
"""

from dataclasses import dataclass
from typing import Iterable, Dict, Any, List, Tuple, Optional

from .types import SensorRecord, ImputedRecord
from src.modules.imputation.imputer import Imputer
from src.modules.sketches.count_min import CountMinSketch
from src.modules.sketches.hll import HyperLogLog
from src.modules.sketches.ams_f2 import AMSF2Sketch
from src.modules.reporting.sink import build_sinks_from_config, ReportSink

@dataclass
class SketchWindowStats:
    region: str
    p_hat: float
    ci_low: float
    ci_high: float
    cms_total: int
    cms_abnormal: int
    heavy_buckets: List[int]
    heavy_bucket_ratio: float
    hll_estimate: int
    ams_f2: float  # 新增：AMS F2 估计

class Pipeline:
    def __init__(self, config: Dict[str, Any]):
        self.cfg = config or {}

        # 插补
        self.imputer = Imputer(self.cfg)

        # CMS
        cm = self.cfg.get("count_min", {}) or {}
        self.cms = CountMinSketch(
            epsilon=float(cm.get("epsilon", 0.001)),
            delta=float(cm.get("delta", 0.01)),
            conservative=bool(cm.get("conservative_update", True)),
        )

        # AMS
        ams_cfg = self.cfg.get("ams", {}) or {}
        self.ams = AMSF2Sketch(
            num_sketches=int(ams_cfg.get("num_sketches", 64)),
            seed=int(ams_cfg.get("seed", 0)),
        )

        # HLL（占位版）
        self.hll = HyperLogLog(precision_m=int(self.cfg.get("hyperloglog", {}).get("precision_m", 16)))

        # 阈值与参数
        th = self.cfg.get("thresholds", {}) or {}
        self.anomaly_reading = float(th.get("anomaly_reading", 145.0))
        self.min_conf_for_hll = float(th.get("min_confidence_for_hll", 0.60))

        self.bucket_width = float(self.cfg.get("bucket_width", 5.0))
        cm_max_bucket_default = int(200 // max(1.0, self.bucket_width))
        self.max_bucket = int(cm.get("max_bucket", cm_max_bucket_default))

        est = self.cfg.get("estimation", {}) or {}
        self.alpha = float(est.get("alpha", 0.05))
        self.heavy_ratio_threshold = float(est.get("heavy_ratio_threshold", 0.05))

        # 区域
        self.regions = list(self.cfg.get("regions", [])) or ["R2", "R3", "R4"]

        # 输出
        self.reporting_sinks: List[ReportSink] = build_sinks_from_config(self.cfg)

        self._window_count = 0

    def process_stream(self, stream: Iterable[SensorRecord], window_id: str) -> List[SketchWindowStats]:
        self._window_count = 0
        for r in stream:
            imp = self.imputer.impute(r)

            bucket = int(float(imp.reading_hat) // float(self.bucket_width))
            key_bucket = (imp.region, bucket)

            # CMS 更新
            self.cms.update(key_bucket, amount=1)

            # AMS 更新（同一键，Rademacher 投影）
            self.ams.update(imp.region, key_bucket, amount=1)

            # HLL 条件插入
            if float(imp.reading_hat) > self.anomaly_reading and float(imp.confidence) >= self.min_conf_for_hll:
                self.hll.add(imp.sensor_id)

            self._window_count += 1

        return []

    def _wilson_interval(self, k: int, n: int, alpha: Optional[float] = None) -> Tuple[float, Tuple[float, float]]:
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

    def get_window_stats_sketch(self) -> List[SketchWindowStats]:
        stats: List[SketchWindowStats] = []

        hll_estimate = int(self.hll.estimate())

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

            p_hat, (ci_low, ci_high) = self._wilson_interval(cms_abnormal, cms_total, alpha=self.alpha)

            # AMS F2 估计
            ams_f2 = float(self.ams.estimate_f2(region))

            stats.append(
                SketchWindowStats(
                    region=region,
                    p_hat=p_hat,
                    ci_low=ci_low,
                    ci_high=ci_high,
                    cms_total=int(cms_total),
                    cms_abnormal=int(cms_abnormal),
                    heavy_buckets=heavy_buckets,
                    heavy_bucket_ratio=heavy_bucket_ratio,
                    hll_estimate=hll_estimate,
                    ams_f2=ams_f2,
                )
            )

        return stats

    def export_sketch(self, window_id: str) -> List[SketchWindowStats]:
        stats = self.get_window_stats_sketch()
        for sink in self.reporting_sinks:
            try:
                sink.write_window_stats(stats, window_id)
            except Exception as e:
                print(f"[Sketch] write_window_stats failed: {e}")
        # 重置 AMS（可选：窗口级重置）
        self.ams.reset()
        return stats