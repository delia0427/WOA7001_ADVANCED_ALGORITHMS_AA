"""
阶段5：局部敏感哈希（LSH，基于随机超平面签名）
- 目的：将相似特征向量聚到同一桶，形成近似“相似簇”，用于后续的簇级统计与异常识别
- 方法：Sign Random Projection（随机超平面，亦称 SimHash）
  - 为每个表生成 num_bits 条随机超平面
  - 向量与每条超平面点积的符号构成签名位（>=0 记为 1，否则 0）
  - 相同签名的向量被划入同一桶
- 接口：
  - insert(vec, item_id): 插入特征向量及其 ID
  - query(vec, max_candidates): 返回同签名桶的候选集合（近邻候选）
  - bucket_stats(top_k, min_size): 统计各表中“大桶”的规模与均值等
"""

from typing import List, Dict, Tuple, Any, Iterable
import random


class SRPLSH:
    def __init__(
        self,
        dim: int,
        num_bits: int = 16,
        num_tables: int = 4,
        seed: int = 42,
    ):
        """
        参数：
        - dim: 特征维度
        - num_bits: 每个表的签名位数（超平面数量），越多分辨率越高、桶更细
        - num_tables: 表数量（不同随机集合），越多碰撞更少、召回更高
        - seed: 随机种子，保证可复现
        """
        self.dim = int(dim)
        self.num_bits = int(num_bits)
        self.num_tables = int(num_tables)
        self._rng = random.Random(seed)

        # 每个表的超平面集合：tables_planes[t][b][d]
        self.tables_planes: List[List[List[float]]] = [
            [
                [self._rng.gauss(0.0, 1.0) for _ in range(self.dim)]
                for _ in range(self.num_bits)
            ]
            for _ in range(self.num_tables)
        ]

        # 桶存储：tables_buckets[t][signature_str] = List[(item_id, vec)]
        self.tables_buckets: List[Dict[str, List[Tuple[str, List[float]]]]] = [
            {} for _ in range(self.num_tables)
        ]

    def _bit_for_plane(self, vec: List[float], plane: List[float]) -> int:
        s = 0.0
        # 点积的符号作为位
        for i in range(self.dim):
            s += float(vec[i]) * float(plane[i])
        return 1 if s >= 0.0 else 0

    def _signature(self, vec: List[float], table_idx: int) -> str:
        """
        为某个表生成签名位串（字符串），例如 '101011...'
        """
        planes = self.tables_planes[table_idx]
        bits = [self._bit_for_plane(vec, p) for p in planes]
        # 字符串形式，便于作为 key
        return "".join("1" if b == 1 else "0" for b in bits)

    def insert(self, vec: List[float], item_id: str) -> None:
        """
        插入特征向量到所有表的对应桶中(imp.features, imp.sensor_id)
        """
        if len(vec) != self.dim:
            raise ValueError(f"Vector dimension mismatch: expected {self.dim}, got {len(vec)}")
        for t in range(self.num_tables):
            sig = self._signature(vec, t)
            bucket = self.tables_buckets[t].setdefault(sig, [])
            bucket.append((item_id, list(vec)))

    def query(self, vec: List[float], max_candidates: int = 100) -> List[str]:
        """
        查询与 vec 签名相同的候选 ID 集合（并去重），上限 max_candidates
        注意：这是近邻候选（召回用），并未做精确距离排序
        """
        if len(vec) != self.dim:
            raise ValueError(f"Vector dimension mismatch: expected {self.dim}, got {len(vec)}")
        candidates: List[str] = []
        seen = set()
        for t in range(self.num_tables):
            sig = self._signature(vec, t)
            bucket = self.tables_buckets[t].get(sig, [])
            for item_id, _ in bucket:
                if item_id not in seen:
                    seen.add(item_id)
                    candidates.append(item_id)
                    if len(candidates) >= max_candidates:
                        return candidates
        return candidates

    def bucket_stats(self, top_k: int = 10, min_size: int = 3) -> List[Dict[str, Any]]:
        """
        输出“大桶”的统计信息：
        - 返回按 size 降序的前 top_k 个桶（跨所有表）
        - 每条包含：table_idx, signature, size, means（各维度均值）
        """
        rows: List[Dict[str, Any]] = []
        for t in range(self.num_tables):
            for sig, items in self.tables_buckets[t].items():
                size = len(items)
                if size < int(min_size):
                    continue
                # 维度均值
                dim = self.dim
                sums = [0.0] * dim
                for _, v in items:
                    for i in range(dim):
                        sums[i] += float(v[i])
                means = [s / float(size) for s in sums]
                rows.append(
                    {
                        "table_idx": t,
                        "signature": sig,
                        "size": size,
                        "means": means,
                        "example_ids": [items[0][0]] if size > 0 else [],
                    }
                )
        rows.sort(key=lambda r: r["size"], reverse=True)
        return rows[: int(top_k)]