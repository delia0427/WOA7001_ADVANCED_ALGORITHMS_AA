"""
阶段10：结果落地与可观察性（Reporting/Export Sinks）

用途
- 在窗口结束时将阶段8/9的统计与告警落地到外部：JSONL/CSV/Stdout/Webhook
- 便于审计、回溯、监控集成（如推送到日志系统或告警平台）

实现
- ReportSink 抽象类：统一 write_window_stats / write_alerts 接口
- JSONLSink：将每条记录作为一行 JSON 写入文件
- CSVSink：将记录写入 CSV（WindowStats 与 Alert 分别一类）
- StdoutSink：打印到控制台（用于开发调试）
- WebhookSink：向 HTTP 端点 POST JSON（简单重试）

注意
- 仅做简化实现；生产环境请考虑批量缓冲、失败重试队列、回压控制、幂等写入等
"""

from __future__ import annotations
from typing import List, Dict, Any, Optional
import json
import csv
import time
import os
import threading
import urllib.request
import urllib.error


def _to_jsonable(obj: Any) -> Dict[str, Any]:
    """
    将 dataclass/对象转换为 JSON 友好的 dict（通过 getattr 读取常用字段）
    适配 Stage8WindowStats 与 Alert
    """
    if obj is None:
        return {}
    # 常见字段集合
    keys = [
        # Stage8WindowStats
        "region", "p_hat", "ci_low", "ci_high", "sample_size", "heavy_bucket_ratio",
        "heavy_buckets", "cms_total", "cms_abnormal", "hll_estimate", "sbf_saturation",
        "evidence_score",
        # Alert
        "window_id", "severity", "score", "reason", "metrics", "thresholds", "timestamp",
    ]
    out = {}
    for k in keys:
        if hasattr(obj, k):
            v = getattr(obj, k)
            # 将不可序列化对象转成基本类型
            if isinstance(v, (list, dict, str, int, float, bool)) or v is None:
                out[k] = v
            else:
                try:
                    out[k] = json.loads(json.dumps(v, default=str))
                except Exception:
                    out[k] = str(v)
    return out


class ReportSink:
    def write_window_stats(self, stats: List[Any], window_id: str) -> None:
        raise NotImplementedError

    def write_alerts(self, alerts: List[Any], window_id: str) -> None:
        raise NotImplementedError


class JSONLSink(ReportSink):
    def __init__(self, path_stats: str, path_alerts: str, ensure_dir: bool = True):
        self.path_stats = path_stats
        self.path_alerts = path_alerts
        if ensure_dir:
            for p in [path_stats, path_alerts]:
                d = os.path.dirname(os.path.abspath(p))
                if d and not os.path.exists(d):
                    os.makedirs(d, exist_ok=True)
        self._lock_stats = threading.Lock()
        self._lock_alerts = threading.Lock()

    def write_window_stats(self, stats: List[Any], window_id: str) -> None:
        ts = time.time()
        with self._lock_stats:
            with open(self.path_stats, "a", encoding="utf-8") as f:
                for s in stats:
                    row = _to_jsonable(s)
                    row["window_id"] = window_id
                    row["write_ts"] = ts
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_alerts(self, alerts: List[Any], window_id: str) -> None:
        ts = time.time()
        with self._lock_alerts:
            with open(self.path_alerts, "a", encoding="utf-8") as f:
                for a in alerts:
                    row = _to_jsonable(a)
                    row["window_id"] = window_id
                    row["write_ts"] = ts
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")


class CSVSink(ReportSink):
    def __init__(self, path_stats: str, path_alerts: str, ensure_dir: bool = True):
        self.path_stats = path_stats
        self.path_alerts = path_alerts
        if ensure_dir:
            for p in [path_stats, path_alerts]:
                d = os.path.dirname(os.path.abspath(p))
                if d and not os.path.exists(d):
                    os.makedirs(d, exist_ok=True)
        # 提前定义列头
        self.stats_fields = [
            "window_id", "region", "p_hat", "ci_low", "ci_high", "sample_size",
            "cms_total", "cms_abnormal", "heavy_bucket_ratio", "hll_estimate",
            "sbf_saturation", "evidence_score"
        ]
        self.alert_fields = [
            "window_id", "region", "severity", "score", "reason",
            "p_hat", "ci_low", "ci_high", "cms_total", "cms_abnormal",
            "heavy_bucket_ratio", "hll_estimate", "sbf_saturation", "timestamp"
        ]
        self._lock_stats = threading.Lock()
        self._lock_alerts = threading.Lock()
        # 若文件不存在则写表头
        for path, fields in [(self.path_stats, self.stats_fields), (self.path_alerts, self.alert_fields)]:
            if not os.path.exists(path):
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=fields)
                    w.writeheader()

    def write_window_stats(self, stats: List[Any], window_id: str) -> None:
        with self._lock_stats:
            with open(self.path_stats, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self.stats_fields)
                for s in stats:
                    row = _to_jsonable(s)
                    row["window_id"] = window_id
                    w.writerow({k: row.get(k, "") for k in self.stats_fields})

    def write_alerts(self, alerts: List[Any], window_id: str) -> None:
        with self._lock_alerts:
            with open(self.path_alerts, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self.alert_fields)
                for a in alerts:
                    row = _to_jsonable(a)
                    row["window_id"] = window_id
                    # 展开 metrics 中的 p_hat 等（若有）
                    metrics = row.get("metrics", {}) or {}
                    row["p_hat"] = metrics.get("p_hat", "")
                    row["ci_low"] = metrics.get("ci_low", "")
                    row["ci_high"] = metrics.get("ci_high", "")
                    row["cms_total"] = metrics.get("cms_total", "")
                    row["cms_abnormal"] = metrics.get("cms_abnormal", "")
                    row["heavy_bucket_ratio"] = metrics.get("heavy_bucket_ratio", "")
                    row["hll_estimate"] = metrics.get("hll_estimate", "")
                    row["sbf_saturation"] = metrics.get("sbf_saturation", "")
                    w.writerow({k: row.get(k, "") for k in self.alert_fields})


class StdoutSink(ReportSink):
    def write_window_stats(self, stats: List[Any], window_id: str) -> None:
        print(f"[Stage10] WindowStats window={window_id} count={len(stats)}")
        for s in stats:
            print("  -", json.dumps(_to_jsonable(s), ensure_ascii=False))

    def write_alerts(self, alerts: List[Any], window_id: str) -> None:
        print(f"[Stage10] Alerts window={window_id} count={len(alerts)}")
        for a in alerts:
            print("  -", json.dumps(_to_jsonable(a), ensure_ascii=False))


class WebhookSink(ReportSink):
    def __init__(self, url_stats: str, url_alerts: str, timeout_sec: float = 5.0, max_retries: int = 2):
        self.url_stats = url_stats
        self.url_alerts = url_alerts
        self.timeout = float(timeout_sec)
        self.max_retries = int(max_retries)

    def _post_json(self, url: str, payload: Any) -> None:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        last_err = None
        for _ in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    _ = resp.read()
                return
            except (urllib.error.HTTPError, urllib.error.URLError) as e:
                last_err = e
                time.sleep(0.3)
        if last_err:
            print(f"[Stage10] Webhook POST failed to {url}: {last_err}")

    def write_window_stats(self, stats: List[Any], window_id: str) -> None:
        payload = {"window_id": window_id, "items": [_to_jsonable(s) for s in stats]}
        self._post_json(self.url_stats, payload)

    def write_alerts(self, alerts: List[Any], window_id: str) -> None:
        payload = {"window_id": window_id, "items": [_to_jsonable(a) for a in alerts]}
        self._post_json(self.url_alerts, payload)


def build_sinks_from_config(cfg: Dict[str, Any]) -> List[ReportSink]:
    """
    根据配置构建 sinks 列表
    示例：
    reporting:
      sinks:
        - type: "jsonl"
          path_stats: "out/stats.jsonl"
          path_alerts: "out/alerts.jsonl"
        - type: "csv"
          path_stats: "out/stats.csv"
          path_alerts: "out/alerts.csv"
        - type: "stdout"
        - type: "webhook"
          url_stats: "https://example.com/stats"
          url_alerts: "https://example.com/alerts"
          timeout_sec: 5
          max_retries: 2
    """
    out: List[ReportSink] = []
    reporting = cfg.get("reporting", {}) or {}
    sinks = reporting.get("sinks", []) or []
    for s in sinks:
        t = (s.get("type") or "").lower()
        if t == "jsonl":
            out.append(JSONLSink(path_stats=s["path_stats"], path_alerts=s["path_alerts"]))
        elif t == "csv":
            out.append(CSVSink(path_stats=s["path_stats"], path_alerts=s["path_alerts"]))
        elif t == "stdout":
            out.append(StdoutSink())
        elif t == "webhook":
            out.append(
                WebhookSink(
                    url_stats=s["url_stats"], url_alerts=s["url_alerts"],
                    timeout_sec=float(s.get("timeout_sec", 5.0)), max_retries=int(s.get("max_retries", 2))
                )
            )
        else:
            print(f"[Stage10] Unknown sink type: {t}")
    return out