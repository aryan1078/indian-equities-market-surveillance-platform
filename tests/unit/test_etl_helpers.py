import sys
import types
from datetime import date, datetime

from cassandra.util import Date as CassandraDate

real_db = sys.modules.get("market_surveillance.db")
fake_db = types.ModuleType("market_surveillance.db")
fake_db.get_cassandra_session = lambda: None
fake_db.pg_connection = lambda: None
sys.modules["market_surveillance.db"] = fake_db

from etl_service.main import _coerce_date, canonical_stage_sector, normalize_timestamp, normalize_trading_date

if real_db is not None:
    sys.modules["market_surveillance.db"] = real_db
else:
    sys.modules.pop("market_surveillance.db", None)


def test_normalize_trading_date_accepts_cassandra_date() -> None:
    assert normalize_trading_date(CassandraDate("2026-03-16")) == date(2026, 3, 16)


def test_normalize_trading_date_accepts_iso_string() -> None:
    assert normalize_trading_date("2026-03-16") == date(2026, 3, 16)


def test_normalize_timestamp_accepts_iso_string() -> None:
    assert normalize_timestamp("2026-03-16T09:19:00+05:30") == datetime.fromisoformat("2026-03-16T09:19:00+05:30")


def test_canonical_stage_sector_prefers_metadata_for_unknown_rows() -> None:
    metadata_lookup = {
        "TATACONSUM.NS": types.SimpleNamespace(sector="Consumer Staples"),
    }

    assert canonical_stage_sector("TATACONSUM.NS", "Unknown", metadata_lookup) == "Consumer Staples"
    assert canonical_stage_sector("TATACONSUM.NS", None, metadata_lookup) == "Consumer Staples"
    assert canonical_stage_sector("TATACONSUM.NS", "Consumer Staples", metadata_lookup) == "Consumer Staples"


def test_coerce_date_accepts_iso_string_and_date() -> None:
    assert _coerce_date("2026-04-03") == date(2026, 4, 3)
    assert _coerce_date(date(2026, 4, 20)) == date(2026, 4, 20)
    assert _coerce_date("") is None
