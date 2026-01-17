"""
阶段0：数据类型定义
- SensorRecord: 原始输入记录（可能含缺失/损坏读数）
- ImputedRecord: 插补后的记录（reading_hat 等）
- WindowStats: 窗口级统计与告警评估所需信息
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any, List


@dataclass
class SensorRecord:
    sensor_id: str
    region: str
    reading: Optional[float]  # 允许缺失/损坏
    confidence: float
    metadata: Dict[str, Any]


@dataclass
class ImputedRecord:
    sensor_id: str
    region: str
    reading_hat: float
    confidence: float
    risk_multiplier: float
    features: List[float]  # 例如 [reading_hat, confidence, recent_delta, risk_multiplier]


@dataclass
class WindowStats:
    window_id: str
    region: str
    anomaly_rate_hat: float
    ci_lower: float
    ci_upper: float
    cluster_id: Optional[str] = None  # 可选：LSH 桶/簇标识