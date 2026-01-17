"""
AMS F2 Sketch (per-region)
- 估计每区域的二阶矩 F2 = sum_i f_i^2，用于衡量重尾/爆发强度
- Turnstile 模型（仅 +1 更新）下，AMS 估计器为多次 Rademacher 哈希的平方和的中位数/均值
"""

from typing import Dict, List, Tuple
import hashlib
import math
import statistics

class AMSF2Sketch:
    def __init__(self, num_sketches: int = 32, seed: int = 0):
        """
        num_sketches: 重复次数（越大方差越低，典型 32~128）
        seed: 哈希种子，保证可复现
        """
        self.num_sketches = int(num_sketches)
        self.seed = int(seed)
        # 每个 region 维护 num_sketches 个累加器 Z_j(region)
        self._Z: Dict[str, List[int]] = {}

    def reset(self):
        self._Z.clear()

    def _sign(self, key: Tuple[str, int], j: int) -> int:
        """
        Rademacher 映射 g_j(key) ∈ {+1, -1}，使用稳定哈希生成
        """
        s = f"{self.seed}:{j}:{key[0]}:{key[1]}"
        h = hashlib.blake2s(s.encode("utf-8"), digest_size=8).digest()
        # 取最后一个字节的最低位作为符号
        bit = h[-1] & 1
        return 1 if bit == 0 else -1

    def update(self, region: str, key: Tuple[str, int], amount: int = 1):
        """
        对键 (region, bucket) 执行 +amount 更新：
        Z_j(region) += g_j(key) * amount
        """
        if region not in self._Z:
            self._Z[region] = [0 for _ in range(self.num_sketches)]
        Zr = self._Z[region]
        for j in range(self.num_sketches):
            Zr[j] += self._sign(key, j) * int(amount)

    def estimate_f2(self, region: str) -> float:
        """
        估计该 region 的 F2，取 {Z_j^2} 的中位数以提升稳健性
        """
        Zr = self._Z.get(region)
        if not Zr:
            return 0.0
        squares = [float(z) * float(z) for z in Zr]
        # 也可返回均值：sum(squares) / len(squares)
        return float(statistics.median(squares))