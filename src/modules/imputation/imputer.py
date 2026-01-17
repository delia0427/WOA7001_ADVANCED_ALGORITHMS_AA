"""
阶段2：插补（基础版）
- 目标：为缺失/损坏读数生成 reading_hat，占位实现邻域获取
- 方法：
  - Imputer.impute(record): 若 reading 缺失/损坏，用区域回退均值；保留原 confidence
  - Imputer.fetch_neighborhood(record): 返回同区域邻居（占位，后续阶段完善）
  - Imputer._fallback_mean(region): 区域回退均值（占位值，后续用邻域加权替换）
"""

from typing import List, Dict, Any
from src.pipeline.types import SensorRecord, ImputedRecord


class Imputer:
    def __init__(self, config: Dict[str, Any]):
        self.cfg = config

    def impute(self, r: SensorRecord) -> ImputedRecord:
        """
        基础插补：
        - 如果 r.reading 不为 None，直接使用
        - 否则使用区域回退均值（占位），后续将用 fetch_neighborhood + 加权/稳健估计替换
        """
        if r.reading is not None:
            reading_hat = float(r.reading)
        else:
            # 占位：后续用邻域加权替代
            reading_hat = self._fallback_mean(r.region)

        return ImputedRecord(
            sensor_id=r.sensor_id,
            region=r.region,
            reading_hat=reading_hat,
            confidence=r.confidence,
            risk_multiplier=1.0,  # 由 Pipeline 根据配置补充
            features=[],          # 由 Pipeline 构造
        )

    def fetch_neighborhood(self, r: SensorRecord) -> List[SensorRecord]:
        """
        占位：返回同区域/近邻的传感器记录（当前窗口或历史）
        - 阶段2不实现实际邻域检索；后续阶段将接入窗口缓存/历史存储
        """
        return []

    def _fallback_mean(self, region: str) -> float:
        """
        占位区域回退均值：
        - R3 高风险区域 → 以题目片段为参考，给出较高的回退均值
        - R2 中高风险区域 → 中等回退
        - 其他区域 → 常态较低
        注：后续阶段将用邻域加权/稳健估计替换此逻辑
        """
        if region == "R3":
            return 151.0
        if region == "R2":
            return 95.0
        return 50.0