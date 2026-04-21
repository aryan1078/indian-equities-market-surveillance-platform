import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from .settings import get_settings


class StockReference(BaseModel):
    symbol: str
    exchange: str
    sector: str = "Unknown"
    company_name: str
    country: str = "India"
    is_active: bool = True
    watchlist: bool = False
    aliases: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


@lru_cache(maxsize=1)
def load_stock_references(path: Path | None = None) -> list[StockReference]:
    settings = get_settings()
    metadata_path = path or settings.metadata_path
    records = json.loads(metadata_path.read_text(encoding="utf-8"))
    return [StockReference.model_validate(record) for record in records]


@lru_cache(maxsize=1)
def sector_lookup() -> dict[str, StockReference]:
    return {stock.symbol: stock for stock in load_stock_references()}


def active_symbols() -> list[str]:
    return [stock.symbol for stock in load_stock_references() if stock.is_active]


def watchlist_symbols() -> list[str]:
    selected = [stock.symbol for stock in load_stock_references() if stock.is_active and stock.watchlist]
    return selected or active_symbols()[:25]


def valid_peer_sector(sector: str | None) -> bool:
    if not sector:
        return False
    return sector.strip().lower() not in {"unknown", "unclassified", "n/a"}


def clear_metadata_cache() -> None:
    load_stock_references.cache_clear()
    sector_lookup.cache_clear()
