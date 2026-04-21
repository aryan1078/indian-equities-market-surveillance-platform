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


SECTOR_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("Banking", (" BANK ", "BANKING", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "AUBANK", "RBLBANK", "SBIN", "CANBK")),
    ("Insurance", (" INSURANCE ", " ASSURANCE ", " REINSURANCE ", " LIFE INS", " GENERAL INS")),
    (
        "Financial Services",
        (
            " FINANCE ",
            " FINANCIAL ",
            " CAPITAL ",
            " FINSERV ",
            " INVEST",
            " SECURIT",
            " WEALTH ",
            " CREDIT ",
            " BROK",
            " ASSET MANAGEMENT ",
            " HOUSING FINANCE ",
            " HFC ",
            " LEASING ",
        ),
    ),
    ("Telecom", (" TELECOM ", " COMMUNICATION", " BROADBAND ", " SATELLITE ", " NETWORK ")),
    (
        "Information Technology",
        (
            " TECHNOLOG",
            " TECH ",
            " TECHNICAL ",
            " INFOTECH",
            " CONSULTANCY ",
            " SOFTWARE ",
            " DIGITAL ",
            " SYSTEMS ",
            " COMPUT",
            " CYBER ",
            " DATA ",
            " SOLUTIONS ",
        ),
    ),
    (
        "Pharmaceuticals",
        (
            " PHARMA ",
            " PHARMACEUTICAL",
            " DRUG",
            " LABS ",
            " LABORATORIES ",
            " BIOTECH",
            " THERAPEUT",
            " FORMULATION",
            " LIFESCI",
        ),
    ),
    ("Healthcare", (" HEALTHCARE ", " HOSPITAL", " MEDICAL ", " DIAGNOST", " CLINIC ", " HEALTH ")),
    ("Automobile", (" AUTO ", " AUTOMOT", " MOTORS ", " MOTOR ", " TYRE ", " TIRES ", " TRACTOR", " MOBILITY ")),
    ("Utilities", (" POWER ", " ELECTRIC", " WATER ", " GRID ", " HYDRO", " SOLAR ", " WIND ", " RENEWABLE ")),
    ("Energy", (" OIL ", " GAS ", " PETRO", " COAL ", " REFIN", " LNG ", " EXPLORATION ", " DRILLING ")),
    (
        "Basic Materials",
        (
            " STEEL ",
            " METAL ",
            " MINING",
            " MINES ",
            " ALLOY",
            " CEMENT ",
            " CHEM",
            " FERT",
            " PAINT",
            " CERAM",
            " COPPER",
            " ALUMIN",
            " POLYMER",
            " PLASTIC",
        ),
    ),
    ("Real Estate", (" REALTY", " REAL ESTATE", " PROPERT", " DEVELOP", " ESTATE ", " REIT ")),
    ("Media", (" MEDIA ", " BROADCAST", " ENTERTAIN", " FILMS ", " MUSIC ", " TV ", " RADIO ")),
    (
        "Consumer Staples",
        (
            " FOOD",
            " FMCG ",
            " TOBACCO",
            " BEVERAGE",
            " BREWER",
            " DISTILL",
            " DAIRY",
            " TEA ",
            " COFFEE",
            " SOAP ",
            " PERSONAL CARE",
            " HOUSEHOLD ",
            " CONSUMER PRODUCTS",
        ),
    ),
    (
        "Consumer Discretionary",
        (
            " RETAIL",
            " APPAREL",
            " FASHION",
            " TEXTILE",
            " FOOTWEAR",
            " JEWEL",
            " LIFESTYLE",
            " HOTELS",
            " TRAVEL",
            " LEISURE",
            " RESTAUR",
            " FURNITURE",
        ),
    ),
    (
        "Industrials",
        (
            " INFRA",
            " ENGINEER",
            " LOGISTICS",
            " PORT ",
            " PORTS ",
            " SHIP",
            " TRANSPORT",
            " AVIATION",
            " AEROSPACE",
            " DEFENCE",
            " DEFENSE",
            " CONSTRUCTION",
            " PROJECTS",
            " MACHIN",
            " INDUSTR",
            " EQUIP",
            " CABLE ",
            " RAIL ",
            " RAILWAY",
            " MARINE ",
        ),
    ),
]


def _identity_text(*values: str | None) -> str:
    normalized_parts: list[str] = []
    for value in values:
        if not value:
            continue
        cleaned = " ".join(str(value).replace("_", " ").replace("-", " ").split()).upper()
        if cleaned:
            normalized_parts.append(cleaned)
    return f" {' '.join(normalized_parts)} "


def infer_sector_from_identity(company_name: str, aliases: list[str] | None = None, symbol: str | None = None) -> str | None:
    haystack = _identity_text(company_name, symbol, *(aliases or []))
    for sector, keywords in SECTOR_KEYWORDS:
        if any(keyword in haystack for keyword in keywords):
            return sector
    return None


@lru_cache(maxsize=1)
def load_stock_references(path: Path | None = None) -> list[StockReference]:
    settings = get_settings()
    metadata_path = path or settings.metadata_path
    records = json.loads(metadata_path.read_text(encoding="utf-8"))
    references: list[StockReference] = []
    for record in records:
        stock = StockReference.model_validate(record)
        if valid_peer_sector(stock.sector):
            references.append(stock)
            continue

        inferred_sector = infer_sector_from_identity(stock.company_name, stock.aliases, stock.symbol)
        if inferred_sector is None:
            references.append(stock)
            continue

        references.append(
            stock.model_copy(
                update={
                    "sector": inferred_sector,
                    "metadata": {
                        **stock.metadata,
                        "sector_source": "keyword_inference",
                    },
                }
            )
        )
    return references


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
