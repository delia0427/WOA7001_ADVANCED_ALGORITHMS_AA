"""
Compare the generated stream (data/stream.jsonl) with reported stats (results/stats.jsonl)
and write a per-region comparison report.

Usage (from main):
    from src.modules.testing.compare import compare_stream_and_stats, write_compare_results
    rows = compare_stream_and_stats(run_dir, cfg)
    write_compare_results(run_dir, rows)
"""

from __future__ import annotations
import json
import os
from collections import defaultdict
from typing import Dict, Any, List, Tuple

from src.pipeline.types import SensorRecord
from src.modules.imputation.imputer import Imputer


def _wilson_interval(k: int, n: int, alpha: float) -> Tuple[float, float, float]:
    if n <= 0:
        return 0.0, 0.0, 0.0
    # match pipeline's constants (alpha=0.05 → z≈1.959964)
    z = 1.959964 if abs(alpha - 0.05) < 1e-9 else 1.644854 if abs(alpha - 0.10) < 1e-9 else 1.959964
    p = k / float(n)
    denom = 1.0 + (z * z) / float(n)
    center = p + (z * z) / (2.0 * float(n))
    delta = z * ((p * (1.0 - p) / float(n)) + (z * z) / (4.0 * float(n * n))) ** 0.5
    low = max(0.0, (center - delta) / denom)
    high = min(1.0, (center + delta) / denom)
    return p, low, high


def _load_stream(stream_path: str) -> List[SensorRecord]:
    out: List[SensorRecord] = []
    with open(stream_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            out.append(
                SensorRecord(
                    sensor_id=obj["sensor_id"],
                    region=obj["region"],
                    reading=obj.get("reading", None),
                    confidence=float(obj["confidence"]),
                    metadata=obj.get("metadata", {}),
                )
            )
    return out


def _load_reported_stats(stats_path: str) -> Dict[str, Dict[str, Any]]:
    """Get the last occurrence per region from stats.jsonl."""
    by_region: Dict[str, Dict[str, Any]] = {}
    if not os.path.exists(stats_path):
        return by_region
    with open(stats_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            r = obj.get("region")
            if r:
                by_region[r] = obj
    return by_region


def compare_stream_and_stats(run_dir: str, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build reference metrics from data/stream.jsonl and compare with results/stats.jsonl.
    Returns a list of comparison rows (one per region).
    """
    data_dir = os.path.join(run_dir, "data")
    results_dir = os.path.join(run_dir, "results")
    stream_path = os.path.join(data_dir, "stream.jsonl")
    stats_path = os.path.join(results_dir, "stats.jsonl")

    bucket_width = float(cfg.get("bucket_width", 5.0))
    cm_cfg = cfg.get("count_min", {}) or {}
    max_bucket = int(cm_cfg.get("max_bucket", int(200 // max(1.0, bucket_width))))
    th = cfg.get("thresholds", {}) or {}
    anomaly_reading = float(th.get("anomaly_reading", 145.0))
    min_conf_for_hll = float(th.get("min_confidence_for_hll", 0.60))
    est = cfg.get("estimation", {}) or {}
    heavy_ratio_threshold = float(est.get("heavy_ratio_threshold", 0.05))
    alpha = float(est.get("alpha", 0.05))

    # Load inputs
    stream = _load_stream(stream_path)
    reported_by_region = _load_reported_stats(stats_path)
    imputer = Imputer(cfg)

    # Build exact reference from the stream (histograms + sets)
    buckets_by_region: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    total_by_region: Dict[str, int] = defaultdict(int)
    abnormal_by_region: Dict[str, int] = defaultdict(int)
    global_distinct_anomaly = set()

    for rec in stream:
        imp = imputer.impute(rec)
        r = imp.region
        b = int(float(imp.reading_hat) // float(bucket_width))
        buckets_by_region[r][b] += 1
        total_by_region[r] += 1
        if (b * bucket_width) >= anomaly_reading:
            abnormal_by_region[r] += 1
        if float(imp.reading_hat) > anomaly_reading and float(imp.confidence) >= min_conf_for_hll:
            global_distinct_anomaly.add(imp.sensor_id)

    rows: List[Dict[str, Any]] = []
    for region in sorted(total_by_region.keys()):
        cms_total_ref = total_by_region[region]
        cms_abnormal_ref = abnormal_by_region[region]
        p_hat_ref, ci_low_ref, ci_high_ref = _wilson_interval(cms_abnormal_ref, cms_total_ref, alpha)

        heavy_ref = []
        if cms_total_ref > 0:
            for b in range(max_bucket + 1):
                f = buckets_by_region[region].get(b, 0)
                if (f / float(cms_total_ref)) >= heavy_ratio_threshold:
                    heavy_ref.append(b)
        heavy_ratio_ref = len(heavy_ref) / float(max(1, (max_bucket + 1)))

        # Exact F2 from histogram
        f2_ref = 0.0
        for f in buckets_by_region[region].values():
            f2_ref += float(f * f)

        hll_ref_global = len(global_distinct_anomaly)

        rep = reported_by_region.get(region, {})
        diffs = {
            "cms_total_diff": cms_total_ref - int(rep.get("cms_total", 0)),
            "cms_abnormal_diff": cms_abnormal_ref - int(rep.get("cms_abnormal", 0)),
            "p_hat_diff": p_hat_ref - float(rep.get("p_hat", 0.0)),
            "ci_low_diff": ci_low_ref - float(rep.get("ci_low", 0.0)),
            "ci_high_diff": ci_high_ref - float(rep.get("ci_high", 0.0)),
            "heavy_bucket_ratio_diff": heavy_ratio_ref - float(rep.get("heavy_bucket_ratio", 0.0)),
            "hll_estimate_diff": hll_ref_global - int(rep.get("hll_estimate", 0)),
            "ams_f2_diff": f2_ref - float(rep.get("ams_f2", 0.0)),
        }

        # 绝对/相对容差
        tol_p = 1e-9
        tol_ci = 1e-6
        tol_ratio = 1e-9
        ams_f2_rel_tol = 0.15  # 建议 0.10~0.15；重复数更大可调小

        # HLL 允许相对误差（若为标准 HLL）
        hll_rel_err = 0.0
        if hll_ref_global > 0:
            hll_rel_err = abs(diffs["hll_estimate_diff"]) / float(hll_ref_global)

        # AMS F2 相对误差
        ams_f2_rel_err = 0.0
        if f2_ref > 0:
            ams_f2_rel_err = abs(diffs["ams_f2_diff"]) / float(f2_ref)

        status = {
            "counts_ok": diffs["cms_total_diff"] == 0 and diffs["cms_abnormal_diff"] == 0,
            "p_hat_ok": abs(diffs["p_hat_diff"]) <= tol_p,
            "ci_ok": abs(diffs["ci_low_diff"]) <= tol_ci and abs(diffs["ci_high_diff"]) <= tol_ci,
            "heavy_ratio_ok": abs(diffs["heavy_bucket_ratio_diff"]) <= tol_ratio,
            "ams_f2_ok": ams_f2_rel_err <= ams_f2_rel_tol,
            "ams_f2_rel_err": ams_f2_rel_err,
            "hll_ok": hll_rel_err <= 0.30,
            "hll_rel_err": hll_rel_err,
        }

        rows.append(
            {
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
                    "cms_total": rep.get("cms_total"),
                    "cms_abnormal": rep.get("cms_abnormal"),
                    "p_hat": rep.get("p_hat"),
                    "ci_low": rep.get("ci_low"),
                    "ci_high": rep.get("ci_high"),
                    "heavy_buckets": rep.get("heavy_buckets"),
                    "heavy_bucket_ratio": rep.get("heavy_bucket_ratio"),
                    "hll_estimate": rep.get("hll_estimate"),
                    "ams_f2": rep.get("ams_f2"),
                },
                "diffs": diffs,
                "status": status,
            }
        )

    return rows


def write_compare_results(run_dir: str, rows: List[Dict[str, Any]]) -> str:
    """Write comparison rows to results/compare.jsonl and return the file path."""
    results_dir = os.path.join(run_dir, "results")
    os.makedirs(results_dir, exist_ok=True)
    out_path = os.path.join(results_dir, "compare.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return out_path