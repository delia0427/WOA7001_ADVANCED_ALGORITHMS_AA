"""
阶段8：蒙特卡洛（自助法）窗口估计与多源证据融合

目标
- 基于 Reservoir 的代表性样本，按区域用 bootstrap 估计异常比例 p̂，并给出置信区间
- 融合 CMS（重频桶）、HLL（异常设备去重）、SBF（可疑短期记忆）等证据，输出综合窗口统计

方法摘要
- Bootstrap（自助法）：
  - 对每个区域，从蓄水池样本按权重（score）进行有放回抽样，重复 B 次
  - 每次计算异常比例（读数 >= anomaly_reading 且可选置信度门槛）
  - 以这些重复的比例分布的均值为 p̂，分位数作为置信区间（例如 95% CI 取 2.5%、97.5% 分位）
- 融合：
  - 统计 CMS 的重频桶占比、HLL 的异常设备数估计、SBF 的饱和度
  - 生成一个简易的组合打分，用于排序与告警参考

注意
- Bootstrap 依赖样本的代表性；建议 Reservoir 使用加权采样（A‑ES）且窗口内样本量足够
- 若样本过少，将退回 Wilson 区间或直接返回宽区间
"""

from dataclasses import dataclass
from typing import List, Tuple, Dict, Any, Optional
import math
import random


@dataclass
class Stage8WindowStats:
    region: str
    p_hat: float
    ci_low: float
    ci_high: float
    sample_size: int
    heavy_bucket_ratio: float
    heavy_buckets: List[int]
    cms_total: int
    cms_abnormal: int
    hll_estimate: int
    sbf_saturation: float
    evidence_score: float  # 综合打分（便于排序/阈值）


class MonteCarloBootstrapEstimator:
    def __init__(
        self,
        alpha: float = 0.05,
        n_bootstrap: int = 500,
        min_samples_for_bootstrap: int = 30,
        rng_seed: int = 2024,
    ):
        """
        参数：
        - alpha: 置信水平显著性（0.05 → 95% CI）
        - n_bootstrap: Bootstrap 重复次数（越大越稳，资源消耗增加）
        - min_samples_for_bootstrap: 每区域最小样本数，不足则回退 Wilson 区间
        - rng_seed: 随机种子
        """
        self.alpha = float(alpha)
        self.B = int(n_bootstrap)
        self.min_n = int(min_samples_for_bootstrap)
        self._rng = random.Random(int(rng_seed))

    def _wilson_interval(self, k: int, n: int, alpha: Optional[float] = None) -> Tuple[float, Tuple[float, float]]:
        """
        Wilson score interval for binomial proportion（回退用）
        """
        alpha = self.alpha if alpha is None else float(alpha)
        if n <= 0:
            return 0.0, (0.0, 0.0)
        z = 1.959964 if abs(alpha - 0.05) < 1e-9 else 1.644854 if abs(alpha - 0.10) < 1e-9 else 1.959964
        p = float(k) / float(n)
        denom = 1.0 + (z * z) / float(n)
        center = p + (z * z) / (2.0 * float(n))
        delta = z * ((p * (1.0 - p) / float(n)) + (z * z) / (4.0 * float(n * n))) ** 0.5
        low = max(0.0, (center - delta) / denom)
        high = min(1.0, (center + delta) / denom)
        return p, (low, high)

    def _bootstrap_proportion(
        self,
        readings: List[float],
        confidences: List[float],
        weights: List[float],
        anomaly_reading: float,
        min_confidence: Optional[float],
        B: int,
    ) -> Tuple[float, float, float]:
        """
        对单一区域的样本做加权 bootstrap，返回 (p_hat, ci_low, ci_high)
        - readings: 插补后读数
        - confidences: 置信度
        - weights: 采样权重（建议用 score）
        - anomaly_reading: 异常阈值（读数下界）
        - min_confidence: 若设定，则异常判定需同时满足置信度 ≥ min_confidence
        - B: bootstrap 次数
        """
        n = len(readings)
        if n == 0:
            return 0.0, 0.0, 0.0

        # 归一化权重为抽样概率
        w_sum = sum(max(0.0, float(w)) for w in weights) or 1.0
        probs = [max(0.0, float(w)) / w_sum for w in weights]

        boot_props: List[float] = []
        for _ in range(B):
            abnormal = 0
            # 有放回抽样 n 次，近似同样大小的样本
            for _j in range(n):
                idx = self._weighted_choice(probs)  # 返回索引
                r = float(readings[idx])
                c = float(confidences[idx])
                is_abnormal = (r >= float(anomaly_reading)) and (min_confidence is None or c >= float(min_confidence))
                abnormal += 1 if is_abnormal else 0
            boot_props.append(abnormal / float(n))

        boot_props.sort()
        p_hat = sum(boot_props) / float(len(boot_props))
        # 取分位作为 CI
        lo_q = self.alpha / 2.0
        hi_q = 1.0 - self.alpha / 2.0
        ci_low = self._quantile(boot_props, lo_q)
        ci_high = self._quantile(boot_props, hi_q)
        return p_hat, ci_low, ci_high

    def _weighted_choice(self, probs: List[float]) -> int:
        """
        依据概率列表进行一次加权抽样，返回索引
        """
        r = self._rng.random()
        acc = 0.0
        for i, p in enumerate(probs):
            acc += p
            if r <= acc:
                return i
        return len(probs) - 1  # 浮点尾部保护

    def _quantile(self, arr: List[float], q: float) -> float:
        """
        简单分位数（线性插值）
        """
        n = len(arr)
        if n == 0:
            return 0.0
        pos = q * (n - 1)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return arr[lo]
        w = pos - lo
        return arr[lo] * (1.0 - w) + arr[hi] * w

    def aggregate_with_evidence(
        self,
        reservoir_samples: List[Any],  # ImputedRecord
        cms,
        regions: List[str],
        bucket_width: float,
        anomaly_reading: float,
        max_bucket: int,
        min_conf_for_anomaly: Optional[float],
        heavy_ratio_threshold: float,
        hll_estimate: int,
        sbf_saturation: float,
    ) -> List[Stage8WindowStats]:
        """
        融合 Reservoir（bootstrap）+ CMS（重频桶）+ HLL + SBF，返回每个区域的综合统计
        """
        # 预聚合样本
        by_region: Dict[str, Dict[str, List[float]]] = {
            r: {"readings": [], "confidences": [], "weights": []} for r in regions
        }
        for imp in reservoir_samples:
            r = getattr(imp, "region", None)
            if r in by_region:
                by_region[r]["readings"].append(float(getattr(imp, "reading_hat")))
                by_region[r]["confidences"].append(float(getattr(imp, "confidence")))
                # 与管线一致的权重（score）
                w = float(getattr(imp, "reading_hat")) * float(getattr(imp, "confidence")) * float(
                    getattr(imp, "risk_multiplier", 1.0)
                )
                by_region[r]["weights"].append(max(0.0, w))

        stats: List[Stage8WindowStats] = []
        # 枚举区域，做 bootstrap + CMS 汇总
        for region in regions:
            readings = by_region[region]["readings"]
            confidences = by_region[region]["confidences"]
            weights = by_region[region]["weights"]
            n = len(readings)

            # CMS 汇总
            cms_total = 0
            cms_abnormal = 0
            heavy_buckets: List[int] = []
            for b in range(int(max_bucket) + 1):
                f = int(cms.query((region, b)))
                cms_total += f
                if float(b) * float(bucket_width) >= float(anomaly_reading):
                    cms_abnormal += f
            heavy_bucket_ratio = 0.0
            if cms_total > 0:
                for b in range(int(max_bucket) + 1):
                    f = int(cms.query((region, b)))
                    if (f / float(cms_total)) >= float(heavy_ratio_threshold):
                        heavy_buckets.append(b)
                heavy_bucket_ratio = len(heavy_buckets) / float(max(1, (max_bucket + 1)))

            # Bootstrap 或 Wilson 回退
            if n >= self.min_n:
                p_hat, ci_low, ci_high = self._bootstrap_proportion(
                    readings=readings,
                    confidences=confidences,
                    weights=weights,
                    anomaly_reading=anomaly_reading,
                    min_confidence=min_conf_for_anomaly,
                    B=self.B,
                )
            else:
                # 用 CMS 的比例作为点估计，Wilson 区间回退
                p_hat, (ci_low, ci_high) = self._wilson_interval(cms_abnormal, cms_total)

            # 组合证据打分（示例，0~1 范围）
            # - 比例 p_hat 直接采纳（已在 0~1）
            # - 重频强度：min(1, cms_abnormal / cms_total)，若 cms_total=0 视为 0
            # - 设备扩散：min(1, hll_estimate / max(1, cms_total))（粗略归一化）
            # - 可疑饱和：sbf_saturation（本身 0~1）
            heavy_intensity = (cms_abnormal / float(cms_total)) if cms_total > 0 else 0.0
            device_spread = min(1.0, float(hll_estimate) / float(max(1, cms_total)))
            evidence_score = (
                0.4 * p_hat
                + 0.25 * heavy_intensity
                + 0.2 * device_spread
                + 0.15 * float(sbf_saturation)
            )

            stats.append(
                Stage8WindowStats(
                    region=region,
                    p_hat=p_hat,
                    ci_low=ci_low,
                    ci_high=ci_high,
                    sample_size=n,
                    heavy_bucket_ratio=heavy_bucket_ratio,
                    heavy_buckets=heavy_buckets,
                    cms_total=cms_total,
                    cms_abnormal=cms_abnormal,
                    hll_estimate=int(hll_estimate),
                    sbf_saturation=float(sbf_saturation),
                    evidence_score=float(evidence_score),
                )
            )

        return stats