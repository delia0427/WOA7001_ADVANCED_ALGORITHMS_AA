"""
阶段4：Count-Min Sketch（近似频次统计）
- 初始化 width/depth（由 ε/δ 决定）
- 支持 conservative update 以减少过估
- 提供 update/query 方法
"""

import math
import random
from typing import Any


class CountMinSketch:

    # CMS 使用 d 行、每行 w 列的计数表。查询时对同一键在每行的位置取最小值作为近似频次。
    # 经典参数设定为：w = ceil(e/ε)、d = ceil(ln(1/δ))，可保证对任意键 x 的估计满足 ĉ(x) ≤ c(x) + εN，且概率 ≥ 1 − δ（N 为总流量）。
    def __init__(self, epsilon: float, delta: float, conservative: bool = True):
        # width 是列。每一行的列数 width 设为 w = ceil(e / ε)，其中 e 是自然常数约 2.71828，ε 是你配置的误差参数。
        # width 决定哈希表的“横向尺寸”，直接影响哈希碰撞率与近似误差上界。
        # width 越大，同一行内键之间的哈希冲突越少，过估偏差越小。
        # 用 e/ε 是经典推导中的常数因子；它让“最小值查询”在多行独立哈希下把过估控制在 ε·N 的量级。
        self.width = math.ceil(math.e / float(epsilon))
        # depth 决定“概率保证”（把过估的概率压到 δ 以下），width 决定“过估幅度”（把过估量界定在 εN 上限）。
        # 二者搭配：w = ⌈e/ε⌉ 控制偏差大小，d = ⌈ln(1/δ)⌉ 控制失败概率。 按 Cormode–Muthukrishnan 的分析，选择 d = ⌈ln(1/δ)⌉ 可以把“估计超过真实值 εN 的概率”控制在 δ 以内。
        # δ = 0.01 → d = ⌈ln(100)⌉ ≈ ⌈4.605…⌉ = 5
        # δ = 0.001 → d = ⌈ln(1000)⌉ ≈ ⌈6.907…⌉ = 7
        # 这也体现了内存与精度的权衡：更小的 δ 需要更多行，空间复杂度 O(d·w) 增大。
        #  d = ⌈ln(1/δ)⌉  delta 是“失败概率”（容忍的错误概率）。将 δ 变小（更严格），需要更大的 d（更多行/更多独立哈希），从而降低因哈希冲突造成的过估偏差的概率。
        self.depth = math.ceil(math.log(1.0 / float(delta)))
        self.conservative = conservative
        self.tables = [[0] * self.width for _ in range(self.depth)]
        self.seeds = [random.randint(1, 1_000_000_007) for _ in range(self.depth)]

    def _hash(self, key: Any, i: int) -> int:
        # 为 Count-Min Sketch 的第 i 行生成列索引，把给定的 key 映射到该行的某一列位置。
        return hash((key, self.seeds[i])) % self.width

    def update(self, key: Any, amount: int = 1) -> None:
        """
        Count-Min Sketch 的“更新逻辑”，包含两种模式：标准更新与保守更新（conservative update）
        :param key:
        :param amount:
        :return:
        """
        # 对同一个 key，用 depth 个独立哈希函数，把它映射到每一行的一个列位置。这样每行都有一个计数器与该 key 关联。
        # self.depth 就是根据配置δ（delta）按照 d = ⌈ln(1/δ)⌉ 生成的行数（什么行？）
        # 根据_hash(self.width)生成列
        idxs = [self._hash(key, i) for i in range(self.depth)]
        if self.conservative:  # 保守更新
            # CMS 的查询是 min_i table[i][hash_i(key)]。为了让查询结果随更新“正好增加 amount”，保守更新把所有相关计数器至少抬到“旧最小值 + amount”，从而保证新最小值 = 旧最小值 + amount。
            current_min = min(self.tables[i][idxs[i]] for i in range(self.depth))
            target = current_min + amount
            for i in range(self.depth):
                j = idxs[i]
                if self.tables[i][j] < target:  # 只提升那些低于目标值的行计数；已经很高的行（可能因为与重频键冲突被抬高）不再继续累加，避免“过度累加”叠加偏差。
                    self.tables[i][j] = target
        else:
            for i in range(self.depth):
                self.tables[i][idxs[i]] += amount

    def query(self, key: Any) -> int:
        return min(self.tables[i][self._hash(key, i)] for i in range(self.depth))