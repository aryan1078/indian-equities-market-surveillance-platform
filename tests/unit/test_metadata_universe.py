from __future__ import annotations

import importlib.util
from pathlib import Path

from market_surveillance.metadata import infer_sector_from_identity, valid_peer_sector


def _sync_module():
    module_path = Path(__file__).resolve().parents[2] / "shared" / "scripts" / "sync_nse_universe.py"
    spec = importlib.util.spec_from_file_location("sync_nse_universe", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_valid_peer_sector_rejects_unknown_groups():
    assert valid_peer_sector("Banking") is True
    assert valid_peer_sector("Unknown") is False
    assert valid_peer_sector("unclassified") is False
    assert valid_peer_sector(None) is False


def test_infer_sector_from_identity_recognizes_common_nse_patterns():
    assert infer_sector_from_identity("HDFC Bank Limited", ["HDFCBANK"]) == "Banking"
    assert infer_sector_from_identity("Tata Consultancy Services Limited", ["TCS"]) == "Information Technology"
    assert infer_sector_from_identity("Sun Pharmaceutical Industries Limited", ["SUNPHARMA"]) == "Pharmaceuticals"
    assert infer_sector_from_identity("Adani Ports and Special Economic Zone Limited", ["ADANIPORTS"]) == "Industrials"


def test_infer_sector_from_identity_uses_overrides_for_ambiguous_large_caps():
    assert infer_sector_from_identity("360 ONE WAM LIMITED", ["360ONE"], "360ONE.NS") == "Financial Services"
    assert infer_sector_from_identity("ABB India Limited", ["ABB"], "ABB.NS") == "Industrials"
    assert infer_sector_from_identity("Ather Energy Limited", ["ATHERENERG"], "ATHERENERG.NS") == "Automobile"
    assert infer_sector_from_identity("Bajaj Consumer Care Limited", ["BAJAJCON"], "BAJAJCON.NS") == "Consumer Staples"


def test_build_records_preserves_watchlist_and_metadata():
    module = _sync_module()
    source_rows = [
        {
            "SYMBOL": "HDFCBANK",
            "NAME OF COMPANY": "HDFC BANK LIMITED",
            "SERIES": "EQ",
            "DATE OF LISTING": "19-MAY-1995",
            "PAID UP VALUE": "1",
            "MARKET LOT": "1",
            "ISIN NUMBER": "INE040A01034",
            "FACE VALUE": "1",
        }
    ]
    existing_records = [
        {
            "symbol": "HDFCBANK.NS",
            "exchange": "NSE",
            "sector": "Banking",
            "company_name": "HDFC Bank",
            "country": "India",
            "is_active": True,
            "watchlist": True,
            "aliases": ["HDFCBANK", "HDFC BANK"],
            "metadata": {"note": "keep"},
        }
    ]

    records = module.build_records(source_rows, existing_records)

    assert len(records) >= 1
    record = next(item for item in records if item["symbol"] == "HDFCBANK.NS")
    assert record["symbol"] == "HDFCBANK.NS"
    assert record["sector"] == "Banking"
    assert record["watchlist"] is True
    assert record["metadata"]["note"] == "keep"
    assert record["metadata"]["source"] == "nse_equity_list"
    assert "HDFC BANK" in record["aliases"]


def test_build_records_preserves_missing_watchlist_nse_name():
    module = _sync_module()
    source_rows = [
        {
            "SYMBOL": "HDFCBANK",
            "NAME OF COMPANY": "HDFC BANK LIMITED",
            "SERIES": "EQ",
            "DATE OF LISTING": "19-MAY-1995",
            "PAID UP VALUE": "1",
            "MARKET LOT": "1",
            "ISIN NUMBER": "INE040A01034",
            "FACE VALUE": "1",
        }
    ]
    existing_records = [
        {
            "symbol": "TATAMOTORS.NS",
            "exchange": "NSE",
            "sector": "Automobile",
            "company_name": "Tata Motors",
            "country": "India",
            "is_active": True,
            "watchlist": True,
            "aliases": ["TATAMOTORS", "TATA MOTORS"],
            "metadata": {"source": "legacy_demo"},
        }
    ]

    records = module.build_records(source_rows, existing_records)
    symbols = {record["symbol"] for record in records}

    assert "TATAMOTORS.NS" in symbols
    preserved = next(record for record in records if record["symbol"] == "TATAMOTORS.NS")
    assert preserved["watchlist"] is True
    assert preserved["metadata"]["preserved_reason"] == "missing_from_nse_equity_list"


def test_build_records_adds_curated_watchlist_fallback_when_missing():
    module = _sync_module()
    source_rows = [
        {
            "SYMBOL": "HDFCBANK",
            "NAME OF COMPANY": "HDFC BANK LIMITED",
            "SERIES": "EQ",
            "DATE OF LISTING": "19-MAY-1995",
            "PAID UP VALUE": "1",
            "MARKET LOT": "1",
            "ISIN NUMBER": "INE040A01034",
            "FACE VALUE": "1",
        }
    ]

    records = module.build_records(source_rows, [])

    preserved = next(record for record in records if record["symbol"] == "TATAMOTORS.NS")
    assert preserved["watchlist"] is True
    assert preserved["metadata"]["source"] == "curated_watchlist"
