"""
Compare generated stream (data/stream.jsonl) against pipeline stats (results/stats.jsonl)
- Rebuild reference metrics using the same rules as the Sketching pipeline
- Report per-region diffs and pass/fail checks

Usage:
  python scripts/compare_stream_vs_stats.py --run-dir out/20260117-1030 --config configs/config.yaml
"""

import argparse
import json
import os
from collections import defaultdict
from typing import Dict, Any, List, Tuple

from src.pipeline.types import SensorRecord
from src.modules.imputation.imputer import Imputer


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        import yaml
        return yaml.safe_load(f)


def wilson_interval(k: int, n: int, alpha: float) -> Tuple[float, float, float]:
    # Same defaults as pipeline; z=1.959964 for alpha=0.05
    if n <= 0:
        return 0.0, 0.0, 0.0
    z = 1.959964 if abs(alpha - 0.05) < 1e-9 else 1.644854 if abs(alpha - 0.10) < 1e-9 else 1.959964
    p = k / float(n)
    denom = 1.0 + (z * z) / float(n)
    center = p + (z * z) / (2.0 * float(n))
    delta = z * ((p * (1.0 - p) / float(n)) + (z * z) / (4.0 * float(n * n))) ** 0.5
    low = max(0.0, (center - delta) / denom)
    high = min(1.0, (center + delta) / denom)
    return p, low, high


def load_stream(stream_path: str) -> List[SensorRecord]:
    records = []
    with open(stream_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            records.append(
                SensorRecord(
                    sensor_id=obj["sensor_id"],
                    region=obj["region"],
                    reading=obj.get("reading", None),
                    confidence=float(obj["confidence"]),
                    metadata=obj.get("metadata", {}),
                )
            )
    return records


def load_stats(stats_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Load latest window stats JSONL and collate by region.
    Assumes one window per run-dir; if multiple windows appended, compares the last N lines per region.
    """
    stats_by_region: Dict[str, Dict[str, Any]] = {}
    with open(stats_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            r = obj.get("region")
            if r:
                stats_by_region[r] = obj
    return stats_by_region


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, help="out/YYYYMMDD-HHMM directory")
    ap.add_argument("--config", required=True, help="path to configs/config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    stream_path = os.path.join(args.run_dir, "data", "stream.jsonl")
    stats_path = os.path.join(args.run_dir, "results", "stats.jsonl")

    # Params aligned with pipeline
    bucket_width = float(cfg.get("bucket_width", 5.0))
    cm_cfg = cfg.get("count_min", {}) or {}
    max_bucket = int(cm_cfg.get("max_bucket", int(200 // max(1.0, bucket_width))))
    anomaly_reading = float(cfg.get("thresholds", {}).get("anomaly_reading", 145.0))
    min_conf_for_hll = float(cfg.get("thresholds", {}).get("min_confidence_for_hll", 0.60))
    heavy_ratio_threshold = float(cfg.get("estimation", {}).get("heavy_ratio_threshold", 0.05))
    alpha = float(cfg.get("estimation", {}).get("alpha", 0.05))

    # Load data
    stream = load_stream(stream_path)
    stats_by_region = load_stats(stats_path)
    imputer = Imputer(cfg)

    # Build reference metrics
    buckets_by_region: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    total_by_region: Dict[str, int] = defaultdict(int)
    abnormal_by_region: Dict[str, int] = defaultdict(int)
    distinct_anomaly_devices: Dict[str, set] = defaultdict(set)  # per-region exact, also compute global
    global_distinct_anomaly = set()

    for rec in stream:
        imp = imputer.impute(rec)
        region = imp.region
        # bucketize
        b = int(float(imp.reading_hat) // float(bucket_width))
        buckets_by_region[region][b] += 1
        total_by_region[region] += 1
        if (b * bucket_width) >= anomaly_reading:
            abnormal_by_region[region] += 1
        # HLL gating exact set
        if float(imp.reading_hat) > anomaly_reading and float(imp.confidence) >= min_conf_for_hll:
            global_distinct_anomaly.add(imp.sensor_id)
            distinct_anomaly_devices[region].add(imp.sensor_id)

    # Compare
    print("Comparison per region (reference vs reported):")
    for region in sorted(total_by_region.keys()):
        cms_total_ref = total_by_region[region]
        cms_abnormal_ref = abnormal_by_region[region]
        p_hat_ref, ci_low_ref, ci_high_ref = wilson_interval(cms_abnormal_ref, cms_total_ref, alpha)

        # heavy buckets ref
        heavy_ref = []
        if cms_total_ref > 0:
            for b in range(max_bucket + 1):
                f = buckets_by_region[region].get(b, 0)
                if (f / float(cms_total_ref)) >= heavy_ratio_threshold:
                    heavy_ref.append(b)
        heavy_ratio_ref = len(heavy_ref) / float(max(1, (max_bucket + 1)))

        # AMS-F2 ref: exact F2 from histogram
        f2_ref = 0.0
        for f in buckets_by_region[region].values():
            f2_ref += float(f * f)

        # HLL ref (global; pipeline默认写全局)
        hll_ref_global = len(global_distinct_anomaly)

        reported = stats_by_region.get(region, {})
        diffs = {
            "cms_total_diff": cms_total_ref - int(reported.get("cms_total", 0)),
            "cms_abnormal_diff": cms_abnormal_ref - int(reported.get("cms_abnormal", 0)),
            "p_hat_diff": p_hat_ref - float(reported.get("p_hat", 0.0)),
            "ci_low_diff": ci_low_ref - float(reported.get("ci_low", 0.0)),
            "ci_high_diff": ci_high_ref - float(reported.get("ci_high", 0.0)),
            "heavy_bucket_ratio_diff": heavy_ratio_ref - float(reported.get("heavy_bucket_ratio", 0.0)),
            "hll_estimate_diff": hll_ref_global - int(reported.get("hll_estimate", 0)),
            "ams_f2_diff": f2_ref - float(reported.get("ams_f2", 0.0)),
        }

        # Simple pass/fail with tolerances:
        tol = {
            "p_hat": 1e-9,
            "ci": 1e-6,
            "counts": 0,
            "heavy_ratio": 1e-9,
            "ams_f2": 1e-6,
            # HLL: allow relative error if using standard HLL (m=16 → ~26%); if placeholder set, expect 0 diff
        }
        hll_rel_err = 0.0
        if hll_ref_global > 0:
            hll_rel_err = abs(diffs["hll_estimate_diff"]) / float(hll_ref_global)

        status = {
            "counts_ok": diffs["cms_total_diff"] == 0 and diffs["cms_abnormal_diff"] == 0,
            "p_hat_ok": abs(diffs["p_hat_diff"]) <= tol["p_hat"],
            "ci_ok": (abs(diffs["ci_low_diff"]) <= tol["ci"]) and (abs(diffs["ci_high_diff"]) <= tol["ci"]),
            "heavy_ratio_ok": abs(diffs["heavy_bucket_ratio_diff"]) <= tol["heavy_ratio"],
            "ams_f2_ok": abs(diffs["ams_f2_diff"]) <= tol["ams_f2"],
            "hll_ok": hll_rel_err <= 0.30,  # accept up to ~30% if standard HLL; adjust if using exact set
        }

        print({
            "region": region,
            "ref": {
                "cms_total": cms_total_ref,
                "cms_abnormal": cms_abnormal_ref,
                "p_hat": p_hat_ref,
                "ci_low": ci_low_ref,
                "ci_high": ci_high_ref,
                "heavy_buckets": heavy_ref,
                "heavy_bucket_ratio": heavy_ratio_ref,
                "hll_estimate(ref_global)": hll_ref_global,
                "ams_f2": f2_ref,
            },
            "reported": {
                "cms_total": reported.get("cms_total"),
                "cms_abnormal": reported.get("cms_abnormal"),
                "p_hat": reported.get("p_hat"),
                "ci_low": reported.get("ci_low"),
                "ci_high": reported.get("ci_high"),
                "heavy_buckets": reported.get("heavy_buckets"),
                "heavy_bucket_ratio": reported.get("heavy_bucket_ratio"),
                "hll_estimate": reported.get("hll_estimate"),
                "ams_f2": reported.get("ams_f2"),
            },
            "diffs": diffs,
            "status": status,
        })


if __name__ == "__main__":
    main()