"""
阶段1：主程序入口（最小可跑）
- 功能：加载配置，构造示例数据流，调用 Pipeline.process_stream 并打印结果
"""
import json
import os
import time
import yaml
from typing import List, Dict, Any
from src.pipeline.pipeline import Pipeline
# from src.pipeline.types import SensorRecord
from src.modules.testing.fake_stream_generator import fake_stream_param  # 确保已添加该模块
from src.modules.testing.compare import compare_stream_and_stats, write_compare_results



def load_config(path: str = "configs/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# def fake_stream() -> List[SensorRecord]:
#     # 用题目片段的示例构造几条记录（本阶段仅演示）
#     return [
#         SensorRecord("S201", "R2", 57, 0.91, {}),
#         SensorRecord("S202", "R2", 141, 0.74, {}),
#         SensorRecord("S203", "R2", 149, 0.18, {"note": "spoof"}),
#         SensorRecord("S204", "R3", None, 0.88, {"note": "missing"}),
#         SensorRecord("S205", "R3", 152, 0.55, {}),
#         SensorRecord("S206", "R3", 150, 0.93, {}),
#         SensorRecord("S207", "R4", 31, 0.97, {}),
#         SensorRecord("S208", "R4", 29, 0.94, {}),
#         SensorRecord("S209", "R2", None, 0.12, {"note": "corrupted"}),
#         SensorRecord("S210", "R3", 153, 0.61, {"note": "spike"}),
#     ]

def build_region_specs() -> Dict[str, Dict[str, Any]]:
    """
    可按需调整各区域的样本量、异常率、重桶等参数。
    """
    return {
        "R2": {
            "n": 300,
            "anomaly_rate": 0.25,
            "normal_mean": 100.0,
            "normal_std": 15.0,
            "heavy": {"buckets": [28, 29], "share": 0.15},  # ~[140,145), [145,150)
        },
        "R3": {
            "n": 400,
            "anomaly_rate": 0.70,
            "normal_mean": 115.0,
            "normal_std": 12.0,
            "heavy": {"buckets": [30], "share": 0.40},      # ~[150,155)
        },
        "R4": {
            "n": 250,
            "anomaly_rate": 0.05,
            "normal_mean": 80.0,
            "normal_std": 10.0,
            # 不注入重桶，期望低异常、低 F2
        },
    }


def main():
    cfg = load_config()
    pipe = Pipeline(cfg)
    specs = build_region_specs()

    stream = fake_stream_param(
        specs,
        anomaly_reading=float(cfg.get("thresholds", {}).get("anomaly_reading", 145.0)),
        bucket_width=float(cfg.get("bucket_width", 5.0)),
        seed=2026,                      # 相同 seed → 结果可复现
        device_pool_per_region=300,
        missing_rate=0.03,
        corrupted_rate=0.02,
        low_conf_rate=0.10,
        high_conf_anomaly_share=0.70,   # 高置信异常占比，用于触发 HLL
    )
    data_dir = getattr(pipe.reporting_sinks[0], "_data_dir", "out")
    out_dir = getattr(pipe.reporting_sinks[0], "_out_dir", "out")
    stream_path = os.path.join(data_dir, "stream.jsonl")
    os.makedirs(data_dir, exist_ok=True)
    with open(stream_path, "w", encoding="utf-8") as f:
        for rec in stream:
            f.write(json.dumps({
                "sensor_id": rec.sensor_id,
                "region": rec.region,
                "reading": rec.reading,
                "confidence": rec.confidence,
                "metadata": rec.metadata,
            }, ensure_ascii=False) + "\n")

    window_id = f"win-{time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())}"
    pipe.process_stream(stream, window_id=window_id)
    stats = pipe.export_sketch(window_id=window_id)

    print("Window stats (per region):")
    for s in stats:
        print({
            "region": s.region,
            "p_hat": s.p_hat,
            "ci_low": s.ci_low,
            "ci_high": s.ci_high,
            "cms_total": s.cms_total,
            "cms_abnormal": s.cms_abnormal,
            "heavy_bucket_ratio": s.heavy_bucket_ratio,
            "heavy_buckets": s.heavy_buckets,
            "hll_estimate": s.hll_estimate,
            "ams_f2": s.ams_f2,
        })
    print("Done. Check out/stats.json and out/stats.csv for sink outputs.")

    # 对比校验：生成 compare.jsonl
    rows = compare_stream_and_stats(run_dir=out_dir, cfg=cfg)
    compare_path = write_compare_results(out_dir, rows)
    print(f"Compare report written: {compare_path}")

    # stats = pipeline.process_stream(fake_stream(), window_id="t0")
    # pipeline.export_sketch(window_id="win-2026-01-17T10:00Z")
    # print("window stats:", stats)


if __name__ == "__main__":
    main()
