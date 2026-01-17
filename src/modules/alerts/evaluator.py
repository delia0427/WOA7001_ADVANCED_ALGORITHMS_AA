"""
阶段9：告警评估与证据融合（AlertEvaluator）

目标
- 将阶段8的综合窗口统计（Stage8WindowStats）转为可操作的告警
- 规则与打分结合：样本量/频次充足、区间下界或点估计超阈值、重频桶与设备扩散等
- 提供冷却（cooldown）机制，避免同区域在短时间内重复告警

输入
- Stage8WindowStats（来自 MonteCarloBootstrapEstimator.aggregate_with_evidence）

输出
- Alert 列表（包含 severity、score、解释文本、关键指标等）

配置键（示例）
alerts:
  min_cms_total: 200
  min_sample_size_for_alert: 30
  min_ci_lower: 0.05
  min_p_hat: 0.08
  min_evidence_score: 0.45
  min_heavy_buckets: 1
  min_hll_estimate: 5
  cooldown_seconds: 300
  severity_thresholds:
    critical: 0.75
    high: 0.60
    medium: 0.45
"""

from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
import time


@dataclass
class Alert:
    region: str
    window_id: str
    severity: str
    score: float
    reason: str
    metrics: Dict[str, Any]  # 打包关键指标（p_hat、CI、CMS/HLL/SBF等）
    thresholds: Dict[str, Any]  # 记录当时的阈值，用于可解释性与回溯
    timestamp: float


class AlertEvaluator:
    def __init__(self, config: Dict[str, Any]):
        self.cfg = config or {}
        alerts_cfg = self.cfg.get("alerts", {})

        # 规则阈值
        self.min_cms_total = int(alerts_cfg.get("min_cms_total", 200))
        self.min_sample_size_for_alert = int(alerts_cfg.get("min_sample_size_for_alert", 30))
        self.min_ci_lower = float(alerts_cfg.get("min_ci_lower", 0.05))
        self.min_p_hat = float(alerts_cfg.get("min_p_hat", 0.08))
        self.min_evidence_score = float(alerts_cfg.get("min_evidence_score", 0.45))
        self.min_heavy_buckets = int(alerts_cfg.get("min_heavy_buckets", 1))
        self.min_hll_estimate = int(alerts_cfg.get("min_hll_estimate", 5))

        # 冷却
        self.cooldown_seconds = int(alerts_cfg.get("cooldown_seconds", 300))

        # 严重级别阈值
        st = alerts_cfg.get(
            "severity_thresholds",
            {"critical": 0.75, "high": 0.60, "medium": 0.45},
        )
        self.severity_thresholds = {
            "critical": float(st.get("critical", 0.75)),
            "high": float(st.get("high", 0.60)),
            "medium": float(st.get("medium", 0.45)),
        }

        # 区域级冷却记录：{region: last_alert_ts}
        self._last_alert_ts: Dict[str, float] = {}

    def _severity_for_score(self, score: float) -> Optional[str]:
        if score >= self.severity_thresholds["critical"]:
            return "critical"
        if score >= self.severity_thresholds["high"]:
            return "high"
        if score >= self.severity_thresholds["medium"]:
            return "medium"
        return None

    def _cooldown_ok(self, region: str, now_ts: float) -> bool:
        last = self._last_alert_ts.get(region, 0.0)
        return (now_ts - last) >= float(self.cooldown_seconds)

    def _record_alert_ts(self, region: str, ts: float) -> None:
        self._last_alert_ts[region] = float(ts)

    def generate_alerts(
        self,
        stats_list: List[Any],  # Stage8WindowStats
        window_id: str,
        now_ts: Optional[float] = None,
    ) -> List[Alert]:
        """
        根据阶段8的综合统计生成告警列表
        - 结合 evidence_score 与规则阈值
        - 冷却控制
        """
        now_ts = float(time.time() if now_ts is None else now_ts)
        alerts: List[Alert] = []

        for ws in stats_list:
            # 最小数据量检查
            enough_flow = int(getattr(ws, "cms_total", 0)) >= self.min_cms_total
            enough_samples = int(getattr(ws, "sample_size", 0)) >= self.min_sample_size_for_alert
            heavy_buckets_cnt = len(getattr(ws, "heavy_buckets", []) or [])
            has_heavy = heavy_buckets_cnt >= self.min_heavy_buckets

            # 比例与区间检查
            p_hat = float(getattr(ws, "p_hat", 0.0))
            ci_low = float(getattr(ws, "ci_low", 0.0))
            ci_high = float(getattr(ws, "ci_high", 0.0))

            # 设备扩散与可疑饱和
            hll_est = int(getattr(ws, "hll_estimate", 0))
            sbf_sat = float(getattr(ws, "sbf_saturation", 0.0))

            evidence_score = float(getattr(ws, "evidence_score", 0.0))
            severity = self._severity_for_score(evidence_score)

            # 规则组合：满足以下之一认为“达到告警线”，再由 evidence_score 定级
            rule_hit = (
                (ci_low >= self.min_ci_lower)  # 置信区间下界已高于阈值
                or (p_hat >= self.min_p_hat and has_heavy)  # 点估计偏高且存在重频桶
                or (evidence_score >= self.min_evidence_score and hll_est >= self.min_hll_estimate)  # 分数高且设备扩散
            )

            if not rule_hit:
                continue  # 未达到触发线

            if severity is None:
                continue  # 分数未达最低级别

            if not self._cooldown_ok(ws.region, now_ts):
                # 仍在冷却窗口内，跳过本次告警
                continue

            # 生成解释文本
            reason_parts = []
            if ci_low >= self.min_ci_lower:
                reason_parts.append(f"CI下界 {ci_low:.3f}≥{self.min_ci_lower:.3f}")
            if p_hat >= self.min_p_hat:
                reason_parts.append(f"比例 p̂={p_hat:.3f}≥{self.min_p_hat:.3f}")
            if has_heavy:
                reason_parts.append(f"重频桶 {heavy_buckets_cnt} 个")
            if hll_est >= self.min_hll_estimate:
                reason_parts.append(f"HLL异常设备数 {hll_est}≥{self.min_hll_estimate}")
            if evidence_score >= self.min_evidence_score:
                reason_parts.append(f"综合分数 {evidence_score:.3f}≥{self.min_evidence_score:.3f}")

            reason = "；".join(reason_parts) if reason_parts else "综合规则触发"

            metrics = {
                "p_hat": p_hat,
                "ci_low": ci_low,
                "ci_high": ci_high,
                "cms_total": int(getattr(ws, "cms_total", 0)),
                "cms_abnormal": int(getattr(ws, "cms_abnormal", 0)),
                "heavy_buckets": list(getattr(ws, "heavy_buckets", []) or []),
                "heavy_bucket_ratio": float(getattr(ws, "heavy_bucket_ratio", 0.0)),
                "hll_estimate": hll_est,
                "sbf_saturation": sbf_sat,
            }
            thresholds = {
                "min_cms_total": self.min_cms_total,
                "min_sample_size_for_alert": self.min_sample_size_for_alert,
                "min_ci_lower": self.min_ci_lower,
                "min_p_hat": self.min_p_hat,
                "min_evidence_score": self.min_evidence_score,
                "min_heavy_buckets": self.min_heavy_buckets,
                "min_hll_estimate": self.min_hll_estimate,
                "severity_thresholds": self.severity_thresholds,
            }

            alert = Alert(
                region=str(getattr(ws, "region", "")),
                window_id=str(window_id),
                severity=str(severity),
                score=evidence_score,
                reason=reason,
                metrics=metrics,
                thresholds=thresholds,
                timestamp=now_ts,
            )
            alerts.append(alert)
            self._record_alert_ts(ws.region, now_ts)

        # 按分数降序
        alerts.sort(key=lambda a: a.score, reverse=True)
        return alerts