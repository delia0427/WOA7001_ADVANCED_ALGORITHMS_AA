"""
阶段1：管线装配与通路打通（最小可跑）
- 目标：提供 Pipeline 类的基础结构与最小处理流程
- 说明：本阶段不依赖具体模块实现（imputer/reservoir/sketches等），仅保留配置与基本方法。
"""

from typing import Iterable, Dict, Any, List, Tuple
import time
from .types import SensorRecord, ImputedRecord
from src.modules.imputation.imputer import Imputer
from src.modules.sampling.reservoir import ReservoirSampler
from src.modules.sketches.count_min import CountMinSketch
from src.modules.sketches.hll import HyperLogLog
from src.modules.lsh.lsh import SRPLSH
from src.modules.filters.stable_bloom import StableBloomFilter
from src.modules.estimation.monte_carlo_bootstrap import MonteCarloBootstrapEstimator, Stage8WindowStats
from src.modules.alerts.evaluator import AlertEvaluator, Alert
from src.modules.reporting.sink import build_sinks_from_config, ReportSink

class Pipeline:
    def __init__(self, config: Dict[str, Any]):
        """
        装配基础配置，预留后续模块的占位。
        """
        self.cfg = config
        self.imputer = Imputer(config)    # 实例化插补模块
        self.reservoir = ReservoirSampler(size=config["reservoir"]["size"])  # 实例化加权蓄水池
        # Count-Min Sketch
        self.cms = CountMinSketch(
            epsilon=config["count_min"]["epsilon"],
            delta=config["count_min"]["delta"],
            conservative=config["count_min"]["conservative_update"],
        )
        # HyperLogLog
        self.hll = HyperLogLog(precision_m=config["hyperloglog"]["precision_m"])

        lsh_cfg = config.get("lsh", {"num_bits": 16, "num_tables": 4, "dim": 4, "seed": 42})
        self.lsh = SRPLSH(
            dim=int(lsh_cfg.get("dim", 4)),
            num_bits=int(lsh_cfg.get("num_bits", 16)),
            num_tables=int(lsh_cfg.get("num_tables", 4)),
            seed=int(lsh_cfg.get("seed", 42)),
        )

        # 稳定布隆过滤器（阶段6）
        sbf_cfg = config.get(
            "stable_bloom",
            {"num_cells": 8192, "num_hashes": 4, "counter_bits": 4, "decay_per_insert": 2, "seed": 1337},
        )
        self.sbf = StableBloomFilter(
            num_cells=int(sbf_cfg.get("num_cells", 8192)),
            num_hashes=int(sbf_cfg.get("num_hashes", 4)),
            counter_bits=int(sbf_cfg.get("counter_bits", 4)),
            decay_per_insert=int(sbf_cfg.get("decay_per_insert", 2)),
            seed=int(sbf_cfg.get("seed", 1337)),
        )
        # 阶段8估计器（bootstrap + 融合）
        est_cfg = config.get("estimation", {})
        self.estimator8 = MonteCarloBootstrapEstimator(
            alpha=float(est_cfg.get("alpha", 0.05)),
            n_bootstrap=int(est_cfg.get("n_bootstrap", 500)),
            min_samples_for_bootstrap=int(est_cfg.get("min_samples_for_bootstrap", 30)),
            rng_seed=int(est_cfg.get("rng_seed", 2024)),
        )
        self.evaluator = AlertEvaluator(config)

        # 结果输出 sinks
        self.reporting_sinks: List[ReportSink] = build_sinks_from_config(config)

        # 记录窗口内的条数（用于阈值比率等）
        self._window_count = 0

    def process_stream(self, stream: Iterable[SensorRecord], window_id: str) -> List[Stage8WindowStats]:
        """
        - 对每条记录执行插补，生成 ImputedRecord
        - 暂不进行后续采样/素描/聚类/估计
        - 返回空统计列表，验证插补通路正常
        """
        self._window_count = 0
        th = self.cfg.get("thresholds", {})
        anomaly_reading = float(th.get("anomaly_reading", 145.0))
        min_conf_for_hll = float(th.get("min_confidence_for_hll", 0.60))
        # SBF 规则阈值：低置信 + 高读数（可与 HLL 互补）
        sbf_low_conf_max = float(th.get("sbf_low_conf_max", 0.50))
        sbf_high_reading_min = float(th.get("sbf_high_reading_min", anomaly_reading))
        for r in stream:
            imp = self.imputer.impute(r)
            # 风险倍数与特征占位，后续阶段完善
            imp.risk_multiplier = self._risk_multiplier(imp.region)
            imp.features = [imp.reading_hat, imp.confidence, 0.0, imp.risk_multiplier]

            score = self._score(imp)
            self.reservoir.insert(imp, weight=score)

            # CMS：构造一个“区域+读数桶”的键，用 Count-Min Sketch 近似统计该键出现的次数，形成每个区域的读数直方图（近似）。
            # 用于快速发现“高频异常指示”，比如 R2/R3 中某个高读数区间反复出现。
            # 键是一个二元组，如 ("R3", 30)，表示 R3 区域内“读数在 [150,155) 区间”的计数项。
            key_bucket = (imp.region, self._bucket(imp.reading_hat))   # imp.reading_hat 离散化
            self.cms.update(key_bucket, amount=1)  # 将该键的频次加 1。Count-Min Sketch 会用多个哈希行更新对应位置，维护该键的“近似频次”。

            # HLL：高读数且置信度达标（去重异常设备计数）
            if imp.reading_hat > anomaly_reading and imp.confidence >= min_conf_for_hll:
                self.hll.add(imp.sensor_id)

            # LSH：插入特征向量（近似相似簇）
            try:
                self.lsh.insert(imp.features, imp.sensor_id)
            except Exception:
                # 维度不匹配等异常时忽略该条，避免影响流处理
                pass

            # SBF：规则捕捉“低置信 + 高读数”的可疑设备（短期记忆，允许误报但会随时间淡出）
            if imp.confidence <= sbf_low_conf_max and imp.reading_hat >= sbf_high_reading_min:
                self.sbf.add(imp.sensor_id)

            self._window_count += 1

            # 当前阶段依旧不生成 WindowStats（后续阶段再汇总）
        return []

    def get_window_stats_stage8(self) -> List[Stage8WindowStats]:
        """
        返回每个区域的综合窗口统计（bootstrap + CMS/HLL/SBF 融合）
        依赖配置：
        - regions: 区域列表
        - bucket_width: 分桶宽度
        - count_min.max_bucket: 最大桶索引
        - thresholds.anomaly_reading: 异常阈值
        - thresholds.min_confidence_for_anomaly: （可选）异常判定的最小置信
        - estimation.heavy_ratio_threshold: 重频桶占比阈值
        """
        regions = list(self.cfg.get("regions", [])) or ["R2", "R3", "R4"]
        bucket_width = float(self.cfg.get("bucket_width", 5.0))
        cm_cfg = self.cfg.get("count_min", {})
        max_bucket = int(cm_cfg.get("max_bucket", int(200 // bucket_width)))
        th = self.cfg.get("thresholds", {})
        anomaly_reading = float(th.get("anomaly_reading", 145.0))
        min_conf_for_anomaly = th.get("min_confidence_for_anomaly", None)
        if min_conf_for_anomaly is not None:
            min_conf_for_anomaly = float(min_conf_for_anomaly)
        heavy_ratio_threshold = float(self.cfg.get("estimation", {}).get("heavy_ratio_threshold", 0.05))

        # 从 Reservoir 获取样本（代表性子集）
        samples: List[ImputedRecord] = self.reservoir.sample()

        # HLL 估计（占位/真实）
        hll_estimate = 0
        if hasattr(self.hll, "estimate"):
            try:
                hll_estimate = int(self.hll.estimate())
            except Exception:
                hll_estimate = 0

        # SBF 饱和度（用于误报/容量监控）
        sbf_saturation = 0.0
        try:
            sbf_stats = self.sbf.stats()
            sbf_saturation = float(sbf_stats.get("saturation", 0.0))
        except Exception:
            sbf_saturation = 0.0

        return self.estimator8.aggregate_with_evidence(
            reservoir_samples=samples,
            cms=self.cms,
            regions=regions,
            bucket_width=bucket_width,
            anomaly_reading=anomaly_reading,
            max_bucket=max_bucket,
            min_conf_for_anomaly=min_conf_for_anomaly,
            heavy_ratio_threshold=heavy_ratio_threshold,
            hll_estimate=hll_estimate,
            sbf_saturation=sbf_saturation,
        )


    def get_lsh_bucket_stats(self, top_k: int = 10, min_size: int = 3) -> List[Dict[str, Any]]:
        """
        返回当前窗口内 LSH 的“大桶”统计（跨所有表），用于观察是否出现相似簇的聚集
        字段：
        - table_idx, signature, size, means（各维度均值）, example_ids（示例 ID）
        """
        return self.lsh.bucket_stats(top_k=top_k, min_size=min_size)

    def is_recently_suspect(self, sensor_id: str) -> bool:
        """
        查询设备是否“近期被标记为可疑”（SBF 近似查询，允许误报、会随时间衰减）
        """
        return self.sbf.contains(sensor_id)

    def sbf_decay_steps(self, steps: int) -> None:
        """
        可选：在窗口结束或低负载时手动触发额外衰减
        """
        self.sbf.decay_many(steps)

    def evaluate_alerts_stage9(self, window_id: str, now_ts: float = None) -> List[Alert]:
        """
        在窗口结束后生成告警列表
        """
        stats = self.get_window_stats_stage8()
        return self.evaluator.generate_alerts(stats_list=stats, window_id=window_id, now_ts=now_ts or time.time())

    def export_stage10(self, window_id: str) -> Tuple[List[Stage8WindowStats], List[Alert]]:
        """
        执行阶段10：
        - 计算窗口统计（阶段8）
        - 评估告警（阶段9）
        - 通过所有 sinks 落地输出（阶段10）
        返回：(stats, alerts)
        """
        stats = self.get_window_stats_stage8()
        alerts = self.evaluator.generate_alerts(stats_list=stats, window_id=window_id, now_ts=time.time())

        for sink in self.reporting_sinks:
            try:
                sink.write_window_stats(stats, window_id)
            except Exception as e:
                print(f"[Stage10] write_window_stats failed: {e}")
            try:
                sink.write_alerts(alerts, window_id)
            except Exception as e:
                print(f"[Stage10] write_alerts failed: {e}")

        return stats, alerts

    def _risk_multiplier(self, region: str) -> float:
        return self.cfg.get("risk_multipliers", {}).get(
            region, self.cfg.get("risk_multipliers", {}).get("default", 1.0)
        )

    def _score(self, rec: ImputedRecord) -> float:
        """
        风险加权分数：
        score = reading_hat × confidence × risk_multiplier
        """
        return float(rec.reading_hat) * float(rec.confidence) * float(rec.risk_multiplier)

    def _bucket(self, reading_hat: float, width: float = 5.0) -> int:
        """
        将读数按固定宽度分桶，作为 CMS 键的一部分
        """
        return int(float(reading_hat) // width)
