"""
阶段3：加权蓄水池采样（Efraimidis–Spirakis，A-ES）
- 目标：在单遍流式处理中，以权重（score）为依据保留代表性样本，包含概率 ∝ weight
- 关键点：
  - 对每个 item 生成随机键：key = u^(1/weight)，其中 u ~ Uniform(0,1)
  - 维护大小为 k 的最小堆（按 key 排序），保留最大的 k 个 key（代表性强）
  - 插入复杂度：O(log k)
"""

import heapq
import random
from typing import Any, List, Tuple


class ReservoirSampler:
    def __init__(self, size: int):
        self.k = size
        # 使用最小堆存储 (key, item)；堆顶为当前最小 key（最弱代表性）
        self._heap: List[Tuple[float, Any]] = []

    def insert(self, item: Any, weight: float) -> None:
        """
        插入一个加权样本：
        - 权重 <= 0 的样本直接忽略
        - 计算 A-ES 键并维护堆
        """
        if weight is None or weight <= 0:
            return
        u = random.random()
        key = u ** (1.0 / float(weight))
        if len(self._heap) < self.k:
            heapq.heappush(self._heap, (key, item))
        else:
            # 如果新键优于堆顶（最小键），则替换堆顶
            if key > self._heap[0][0]:
                heapq.heapreplace(self._heap, (key, item))

    def sample(self) -> List[Any]:
        """
        返回当前窗口的样本列表，按代表性从高到低（key 从大到小）排序
        """
        return [it for _, it in sorted(self._heap, key=lambda x: -x[0])]