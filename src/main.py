"""
阶段1：主程序入口（最小可跑）
- 功能：加载配置，构造示例数据流，调用 Pipeline.process_stream 并打印结果
"""

import yaml
from typing import List
from src.pipeline.pipeline import Pipeline
from src.pipeline.types import SensorRecord


def load_config(path: str = "configs/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fake_stream() -> List[SensorRecord]:
    # 用题目片段的示例构造几条记录（本阶段仅演示）
    return [
        SensorRecord("S201", "R2", 57, 0.91, {}),
        SensorRecord("S202", "R2", 141, 0.74, {}),
        SensorRecord("S203", "R2", 149, 0.18, {"note": "spoof"}),
        SensorRecord("S204", "R3", None, 0.88, {"note": "missing"}),
        SensorRecord("S205", "R3", 152, 0.55, {}),
        SensorRecord("S206", "R3", 150, 0.93, {}),
        SensorRecord("S207", "R4", 31, 0.97, {}),
        SensorRecord("S208", "R4", 29, 0.94, {}),
        SensorRecord("S209", "R2", None, 0.12, {"note": "corrupted"}),
        SensorRecord("S210", "R3", 153, 0.61, {"note": "spike"}),
    ]


def main():
    cfg = load_config()
    pipeline = Pipeline(cfg)
    stats = pipeline.process_stream(fake_stream(), window_id="t0")
    stats, alerts = pipeline.export_stage10(window_id="win-2026-01-17T10:00Z")
    print("window stats:", stats)


if __name__ == "__main__":
    main()
