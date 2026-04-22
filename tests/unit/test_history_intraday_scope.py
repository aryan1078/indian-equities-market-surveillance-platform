from types import SimpleNamespace

from market_surveillance.history import significant_intraday_symbols
from market_surveillance.metadata import StockReference


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *_args, **_kwargs):
        return _FakeResult(self._rows)


def _reference(symbol: str, *, watchlist: bool = False) -> StockReference:
    return StockReference(
        symbol=symbol,
        exchange="NSE",
        sector="Banking",
        company_name=symbol.replace(".NS", ""),
        is_active=True,
        watchlist=watchlist,
    )


def test_significant_intraday_symbols_keeps_watchlist_and_recent_liquidity(monkeypatch):
    references = [
        _reference("HDFCBANK.NS", watchlist=True),
        _reference("ICICIBANK.NS", watchlist=True),
        _reference("RELIANCE.NS"),
        _reference("SBIN.NS"),
        _reference("TCS.NS"),
        _reference("INFY.NS"),
    ]
    ranked_rows = [
        {"symbol": "RELIANCE.NS"},
        {"symbol": "SBIN.NS"},
        {"symbol": "TCS.NS"},
        {"symbol": "INFY.NS"},
    ]

    monkeypatch.setattr("market_surveillance.history.get_settings", lambda: SimpleNamespace(
        intraday_default_universe_size=5,
        intraday_ranking_lookback_sessions=20,
    ))
    monkeypatch.setattr("market_surveillance.history.load_stock_references", lambda: references)
    monkeypatch.setattr("market_surveillance.history.watchlist_symbols", lambda: ["HDFCBANK.NS", "ICICIBANK.NS"])
    monkeypatch.setattr("market_surveillance.history.pg_connection", lambda: _FakeConnection(ranked_rows))

    selected = significant_intraday_symbols()

    assert selected == ["HDFCBANK.NS", "ICICIBANK.NS", "RELIANCE.NS", "SBIN.NS", "TCS.NS"]


def test_significant_intraday_symbols_falls_back_to_active_universe_when_daily_rank_missing(monkeypatch):
    references = [
        _reference("HDFCBANK.NS", watchlist=True),
        _reference("ICICIBANK.NS"),
        _reference("RELIANCE.NS"),
        _reference("SBIN.NS"),
    ]

    monkeypatch.setattr("market_surveillance.history.get_settings", lambda: SimpleNamespace(
        intraday_default_universe_size=4,
        intraday_ranking_lookback_sessions=20,
    ))
    monkeypatch.setattr("market_surveillance.history.load_stock_references", lambda: references)
    monkeypatch.setattr("market_surveillance.history.watchlist_symbols", lambda: ["HDFCBANK.NS"])
    monkeypatch.setattr("market_surveillance.history.pg_connection", lambda: _FakeConnection([]))

    selected = significant_intraday_symbols()

    assert selected == ["HDFCBANK.NS", "ICICIBANK.NS", "RELIANCE.NS", "SBIN.NS"]
