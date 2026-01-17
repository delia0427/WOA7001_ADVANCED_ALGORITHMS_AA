"""
阶段6：稳定布隆过滤器（Stable Bloom Filter, SBF）

用途
- 标记并“短期记忆”可疑设备（例如：低置信 + 高读数），用于在线抑制/加权或作为告警辅助证据
- 与普通计数布隆不同，SBF 会在每次插入时随机衰减一部分计数，使旧元素逐渐被遗忘，空间长期稳定

实现要点（简化版）
- 计数数组（长度 m）：每格为 b 位计数器，范围 [0, 2^b - 1]
- 插入：
  1) 随机选择 decay_per_insert 个位置，若计数>0 则减一（全局“老化”）
  2) 对 item 的 k 个哈希位置，将计数设置为最大值 max_counter
- 查询 contains(item)：若 k 个哈希位置的计数都 > 0，则判为“可能在近期出现过”（允许误报）
- 哈希：使用 SHA-256 双重哈希（double hashing）生成 k 个位置，保证跨运行稳定
"""

from typing import List, Dict, Any
import hashlib
import random


class StableBloomFilter:
    def __init__(
        self,
        num_cells: int = 8192,
        num_hashes: int = 4,
        counter_bits: int = 4,
        decay_per_insert: int = 2,
        seed: int = 1337,
    ) -> None:
        assert num_cells > 0
        assert 1 <= num_hashes <= 16
        assert 2 <= counter_bits <= 16
        assert decay_per_insert >= 0

        self.m = int(num_cells)
        self.k = int(num_hashes)
        self.b = int(counter_bits)
        self.decay = int(decay_per_insert)
        self.max_counter = (1 << self.b) - 1
        self.counters: List[int] = [0] * self.m

        self.seed = int(seed)
        self._rng = random.Random(self.seed)

    def _hash_pair(self, item: str) -> (int, int):
        """
        生成两个 64-bit 稳定哈希，供 double hashing 使用：
        pos_j = (h1 + j * h2) % m
        """
        if not isinstance(item, (str, bytes)):
            item = str(item)

        data = item.encode("utf-8") if isinstance(item, str) else item
        salt = self.seed.to_bytes(8, "little", signed=False)

        h = hashlib.sha256(salt + b"/" + data).digest()
        h1 = int.from_bytes(h[:8], "big", signed=False)
        h2 = int.from_bytes(h[8:16], "big", signed=False) or 0x9e3779b97f4a7c15  # 避免为 0
        return h1, h2

    def _positions(self, item: str) -> List[int]:
        h1, h2 = self._hash_pair(item)
        m = self.m
        return [int((h1 + i * h2) % m) for i in range(self.k)]

    def add(self, item: str) -> None:
        """
        插入一个元素：
        - 先全局老化 decay 次（随机选择位置、计数>0 则减一）
        - 再将元素的 k 个位置的计数置为最大值
        """
        # 老化（衰减）
        for _ in range(self.decay):
            idx = self._rng.randrange(self.m)
            if self.counters[idx] > 0:
                self.counters[idx] -= 1

        # 置位为“新鲜”
        maxc = self.max_counter
        for idx in self._positions(item):
            self.counters[idx] = maxc

    def contains(self, item: str) -> bool:
        """
        查询近似包含：所有 k 个位置计数 > 0 则返回 True（可能存在），否则必定不存在
        """
        return all(self.counters[idx] > 0 for idx in self._positions(item))

    def decay_many(self, steps: int) -> None:
        """
        手动触发多步老化（例如窗口结束时额外衰减）
        """
        for _ in range(int(max(0, steps))):
            idx = self._rng.randrange(self.m)
            if self.counters[idx] > 0:
                self.counters[idx] -= 1

    def saturation(self) -> float:
        """
        返回非零计数比例（0.0 ~ 1.0），用于观察“饱和度”
        """
        nz = sum(1 for v in self.counters if v > 0)
        return nz / float(self.m)

    def stats(self) -> Dict[str, Any]:
        nz = sum(1 for v in self.counters if v > 0)
        return {
            "num_cells": self.m,
            "num_hashes": self.k,
            "counter_bits": self.b,
            "max_counter": self.max_counter,
            "decay_per_insert": self.decay,
            "nonzero_cells": nz,
            "saturation": nz / float(self.m),
            "seed": self.seed,
        }