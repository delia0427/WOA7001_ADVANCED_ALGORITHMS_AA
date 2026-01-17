"""
阶段4：HyperLogLog（不同异常传感器的基数估计）
- 简化占位实现：使用集合近似，提供 add/estimate 方法
- 后续可替换为真实 HLL/HLL++ 寄存器与偏差校正
"""

class HyperLogLog:
    def __init__(self, precision_m: int = 16):
        self.m = precision_m
        self._seen = set()

    def add(self, item_id: str) -> None:
        self._seen.add(item_id)

    def estimate(self) -> int:
        # 占位：返回去重后的数量
        return len(self._seen)