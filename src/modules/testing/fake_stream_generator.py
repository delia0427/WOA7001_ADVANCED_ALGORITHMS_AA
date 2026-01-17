"""
Parameterized fake stream generator for Sketching pipeline (Count-Min + AMS + HLL)

Purpose
- Generate controllable test data per region to validate:
  - CMS totals/abnormal counts and heavy_buckets behavior near anomaly thresholds
  - HLL distinct anomalous device counting under confidence gating
  - AMS-F2 sensitivity to heavy-tail / bursty distributions
- Cover larger window sizes to narrow Wilson CI and stress sinks writing

Usage
- See scripts/run_sketch_test.py for example.
"""

from typing import Dict, Any, Iterable, List, Optional, Tuple
import random
import math

from src.pipeline.types import SensorRecord


def _reading_from_bucket(bucket_idx: int, bucket_width: float, jitter: float = 0.5) -> float:
    """
    Uniform reading inside the bucket with tiny jitter to avoid boundary artifacts.
    """
    low = bucket_idx * bucket_width
    high = low + bucket_width
    base = random.uniform(low, high)
    return float(base + random.uniform(-jitter, jitter))


def _sample_normal(mean: float, std: float) -> float:
    return float(random.gauss(mean, std))


def fake_stream_param(
    region_specs: Dict[str, Dict[str, Any]],
    *,
    anomaly_reading: float = 145.0,
    bucket_width: float = 5.0,
    device_pool_per_region: int = 200,
    missing_rate: float = 0.03,
    corrupted_rate: float = 0.02,
    low_conf_rate: float = 0.10,
    high_conf_anomaly_share: float = 0.70,
    seed: int = 2026,
) -> List[SensorRecord]:
    """
    Generate a list of SensorRecord across regions with controllable distributions.

    region_specs example:
    {
      "R2": {
        "n": 300,
        "anomaly_rate": 0.25,
        "normal_mean": 100.0,
        "normal_std": 15.0,
        "heavy": {"buckets": [28, 29], "share": 0.15}  # optional; bucket index based on bucket_width
      },
      "R3": {
        "n": 400,
        "anomaly_rate": 0.70,
        "normal_mean": 115.0,
        "normal_std": 12.0,
        "heavy": {"buckets": [30], "share": 0.40}
      },
      "R4": {
        "n": 250,
        "anomaly_rate": 0.05,
        "normal_mean": 80.0,
        "normal_std": 10.0
      }
    }

    Returns: List[SensorRecord]
    """
    random.seed(int(seed))
    stream: List[SensorRecord] = []

    for region, spec in region_specs.items():
        n = int(spec.get("n", 200))
        anomaly_rate = float(spec.get("anomaly_rate", 0.1))
        normal_mean = float(spec.get("normal_mean", 100.0))
        normal_std = float(spec.get("normal_std", 15.0))
        heavy_conf: Optional[Dict[str, Any]] = spec.get("heavy", None)

        # Precompute heavy bucket behavior
        heavy_buckets: List[int] = []
        heavy_share = 0.0
        if isinstance(heavy_conf, dict):
            heavy_buckets = list(heavy_conf.get("buckets", []))
            heavy_share = float(heavy_conf.get("share", 0.0))

        for i in range(n):
            is_anomaly = (random.random() < anomaly_rate)
            metadata: Dict[str, Any] = {"scenario": "generated"}

            # Decide device id to influence HLL cardinality.
            dev_id = f"{region}-D{random.randint(1, device_pool_per_region)}"

            # Confidence
            if is_anomaly:
                # High confidence anomalies dominate (to trigger HLL), but allow some low-confidence anomalies
                if random.random() < high_conf_anomaly_share:
                    confidence = random.uniform(0.70, 0.95)
                else:
                    confidence = random.uniform(0.10, 0.55)
            else:
                # Normal records tend to have moderate-high confidence
                confidence = random.uniform(0.60, 0.97)

            # Low-confidence override (independent of anomaly) to test gating
            if random.random() < low_conf_rate:
                confidence = min(confidence, random.uniform(0.05, 0.40))
                metadata["note"] = "forced_low_conf"

            # Missing/corrupted handling
            if random.random() < missing_rate:
                reading = None
                metadata["note"] = metadata.get("note", "") + "|missing"
            elif random.random() < corrupted_rate:
                # Corrupted: extreme jitter or None occasionally
                if random.random() < 0.5:
                    reading = None
                else:
                    reading = float(random.uniform(0, anomaly_reading * 1.3))
                metadata["note"] = metadata.get("note", "") + "|corrupted"
            else:
                # Generate reading
                if is_anomaly:
                    # Prefer heavy buckets if configured; else sample just above threshold
                    if heavy_buckets and random.random() < heavy_share:
                        b = random.choice(heavy_buckets)
                        reading = _reading_from_bucket(b, bucket_width)
                    else:
                        # Sample around anomaly_reading with positive skew
                        base = anomaly_reading + abs(random.gauss(5.0, 8.0))
                        reading = float(base)
                else:
                    # Normal distribution below threshold; allow heavy buckets even for normal if specified (rare)
                    if heavy_buckets and random.random() < (heavy_share * 0.2):
                        b = random.choice(heavy_buckets)
                        reading = _reading_from_bucket(b, bucket_width)
                    else:
                        reading = _sample_normal(normal_mean, normal_std)

                # Clip readings to sensible range
                reading = float(max(0.0, min(reading, anomaly_reading * 2.0)))

            stream.append(SensorRecord(sensor_id=dev_id, region=region, reading=reading, confidence=float(confidence), metadata=metadata))

    random.shuffle(stream)
    return stream