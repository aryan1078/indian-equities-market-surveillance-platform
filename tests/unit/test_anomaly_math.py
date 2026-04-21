from anomaly_engine.math_engine import StreamingStats, ewma_alpha, update_ewma, z_score


def test_ewma_alpha_is_positive() -> None:
    assert 0 < ewma_alpha(20) < 1


def test_update_ewma_updates_mean_and_variance() -> None:
    mean, variance = update_ewma(0.0, 0.0, 1.5, 0.2)
    assert mean > 0
    assert variance > 0


def test_z_score_returns_zero_for_tiny_variance() -> None:
    assert z_score(2.0, 1.0, 0.0) == 0.0


def test_streaming_stats_defaults() -> None:
    stats = StreamingStats()
    assert stats.sample_count == 0
    assert stats.last_close is None

