from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class StreamingStats:
    sample_count: int = 0
    last_close: float | None = None
    return_mean: float = 0.0
    return_variance: float = 0.0
    volume_mean: float = 0.0
    volume_variance: float = 0.0


def ewma_alpha(warmup_minutes: int) -> float:
    return 1 - math.exp(math.log(0.5) / max(warmup_minutes, 1))


def update_ewma(mean: float, variance: float, observation: float, alpha: float) -> tuple[float, float]:
    next_mean = alpha * observation + (1 - alpha) * mean
    centered = observation - next_mean
    next_variance = alpha * (centered**2) + (1 - alpha) * variance
    return next_mean, max(next_variance, 1e-12)


def z_score(value: float, mean: float, variance: float) -> float:
    if variance <= 1e-12:
        return 0.0
    return (value - mean) / math.sqrt(variance)

