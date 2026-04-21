from datetime import UTC, datetime, timedelta

from contagion_engine.main import ObservationWindow, flush_expired


def test_flush_expired_can_suppress_live_side_effects(monkeypatch):
    captured: list[tuple[str, bool, bool]] = []

    def fake_write_event(event, *, update_live_cache=True, emit_alerts=True):
        captured.append((event.event_id, update_live_cache, emit_alerts))

    monkeypatch.setattr("contagion_engine.main.write_event", fake_write_event)

    now = datetime(2026, 4, 20, 10, 0, tzinfo=UTC)
    window = ObservationWindow(
        trigger_symbol="SBIN.NS",
        trigger_sector="Banking",
        start=now - timedelta(minutes=5),
        end=now,
        trigger_score=3.1,
        source_run_id="recompute-test",
        event_id="evt-1",
        affected_symbols={"ICICIBANK.NS"},
        peer_scores=[2.4],
    )

    active = {"SBIN.NS": window}
    flush_expired(active, now, update_live_cache=False, emit_alerts=False)

    assert active == {}
    assert captured == [("evt-1", False, False)]
