from __future__ import annotations

import argparse
import csv
import json
from io import StringIO
from pathlib import Path
from urllib.request import Request, urlopen


NSE_EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
CURATED_WATCHLIST_FALLBACKS = [
    {
        "symbol": "TATAMOTORS.NS",
        "exchange": "NSE",
        "sector": "Automobile",
        "company_name": "Tata Motors",
        "country": "India",
        "is_active": True,
        "watchlist": True,
        "aliases": ["TATAMOTORS", "TATA MOTORS"],
        "metadata": {
            "source": "curated_watchlist",
            "nse_symbol": "TATAMOTORS",
            "series": "EQ",
            "preserved_reason": "missing_from_nse_equity_list",
        },
    }
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync the local stock metadata file to the official NSE equity universe")
    parser.add_argument("--input", default="shared/metadata/stocks.json")
    parser.add_argument("--output", default="shared/metadata/stocks.json")
    parser.add_argument("--url", default=NSE_EQUITY_LIST_URL)
    return parser.parse_args()


def symbol_base(symbol: str) -> str:
    normalized = symbol.strip().upper()
    for suffix in (".NS", ".BO"):
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


def clean_aliases(values: list[str]) -> list[str]:
    seen: set[str] = set()
    aliases: list[str] = []
    for value in values:
        normalized = " ".join(str(value).replace("_", " ").split()).upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        aliases.append(normalized)
    return aliases


def load_existing(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_rows(url: str) -> list[dict[str, str]]:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*"})
    with urlopen(request, timeout=30) as response:
        payload = response.read().decode("utf-8-sig")
    reader = csv.DictReader(StringIO(payload))
    rows: list[dict[str, str]] = []
    for row in reader:
        rows.append({str(key).strip(): str(value).strip() for key, value in row.items() if key})
    return rows


def _preserved_watchlist_records(existing_records: list[dict], source_bases: set[str]) -> list[dict]:
    preserved: list[dict] = []
    for item in existing_records:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol.endswith(".NS"):
            continue
        if not item.get("watchlist"):
            continue
        if not item.get("is_active", True):
            continue
        if symbol_base(symbol) in source_bases:
            continue

        metadata = dict(item.get("metadata", {}))
        metadata["source"] = metadata.get("source", "legacy_watchlist")
        metadata["preserved_reason"] = "missing_from_nse_equity_list"
        preserved.append(
            {
                "symbol": symbol,
                "exchange": "NSE",
                "sector": str(item.get("sector") or "Unknown"),
                "company_name": str(item.get("company_name") or symbol),
                "country": str(item.get("country") or "India"),
                "is_active": True,
                "watchlist": True,
                "aliases": clean_aliases([symbol_base(symbol), *(item.get("aliases", []) or [])]),
                "metadata": metadata,
            }
        )
    return preserved


def _curated_watchlist_fallbacks(source_bases: set[str], records: list[dict]) -> list[dict]:
    current_symbols = {str(item.get("symbol", "")).upper() for item in records}
    fallbacks: list[dict] = []
    for item in CURATED_WATCHLIST_FALLBACKS:
        symbol = str(item["symbol"]).upper()
        if symbol in current_symbols:
            continue
        if symbol_base(symbol) in source_bases:
            continue
        fallbacks.append(
            {
                **item,
                "aliases": clean_aliases([symbol_base(symbol), *(item.get("aliases", []) or [])]),
            }
        )
    return fallbacks


def build_records(source_rows: list[dict[str, str]], existing_records: list[dict]) -> list[dict]:
    by_base = {symbol_base(item["symbol"]): item for item in existing_records}
    by_name = {str(item.get("company_name", "")).strip().upper(): item for item in existing_records}
    legacy_watchlist_bases = set()
    if len(existing_records) <= 100:
        legacy_watchlist_bases = {symbol_base(item["symbol"]) for item in existing_records if item.get("is_active", True)}

    records: list[dict] = []
    source_bases: set[str] = set()
    for row in source_rows:
        base_symbol = row["SYMBOL"].upper()
        source_bases.add(base_symbol)
        symbol = f"{base_symbol}.NS"
        company_name = row["NAME OF COMPANY"].strip()
        existing = by_base.get(base_symbol) or by_name.get(company_name.upper())
        existing_metadata = dict(existing.get("metadata", {})) if existing else {}
        aliases = clean_aliases(
            [
                base_symbol,
                company_name,
                *(existing.get("aliases", []) if existing else []),
            ]
        )

        watchlist = bool(existing.get("watchlist")) if existing else False
        if not watchlist and base_symbol in legacy_watchlist_bases:
            watchlist = True

        sector = "Unknown"
        if existing:
            sector = str(existing.get("sector") or "Unknown")

        record = {
            "symbol": symbol,
            "exchange": "NSE",
            "sector": sector,
            "company_name": existing.get("company_name", company_name) if existing else company_name,
            "country": "India",
            "is_active": True,
            "watchlist": watchlist,
            "aliases": aliases,
            "metadata": {
                **existing_metadata,
                "source": "nse_equity_list",
                "source_url": NSE_EQUITY_LIST_URL,
                "nse_symbol": base_symbol,
                "series": row.get("SERIES", ""),
                "listing_date": row.get("DATE OF LISTING", ""),
                "paid_up_value": row.get("PAID UP VALUE", ""),
                "market_lot": row.get("MARKET LOT", ""),
                "isin": row.get("ISIN NUMBER", ""),
                "face_value": row.get("FACE VALUE", ""),
            },
        }
        records.append(record)

    records.extend(_preserved_watchlist_records(existing_records, source_bases))
    records.extend(_curated_watchlist_fallbacks(source_bases, records))
    records.sort(key=lambda item: item["symbol"])
    return records


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    existing_records = load_existing(input_path)
    source_rows = fetch_rows(args.url)
    records = build_records(source_rows, existing_records)
    output_path.write_text(json.dumps(records, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    watchlist_count = sum(1 for record in records if record.get("watchlist"))
    print(f"synced {len(records)} NSE symbols to {output_path}")
    print(f"watchlist symbols preserved: {watchlist_count}")


if __name__ == "__main__":
    main()
