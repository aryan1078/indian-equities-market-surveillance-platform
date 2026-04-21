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


SECTOR_SYMBOL_OVERRIDES: dict[str, str] = {
    "360ONE.NS": "Financial Services",
    "3MINDIA.NS": "Industrials",
    "AARTISURF.NS": "Basic Materials",
    "ABB.NS": "Industrials",
    "ABBOTINDIA.NS": "Pharmaceuticals",
    "ACC.NS": "Basic Materials",
    "ADANIENT.NS": "Industrials",
    "AEGISVOPAK.NS": "Energy",
    "ANGELONE.NS": "Financial Services",
    "ASHOKLEY.NS": "Automobile",
    "ATHERENERG.NS": "Automobile",
    "AURUM.NS": "Real Estate",
    "AVANTIFEED.NS": "Consumer Staples",
    "AWL.NS": "Consumer Staples",
    "BAJAJCON.NS": "Consumer Staples",
    "BEL.NS": "Industrials",
    "CARERATING.NS": "Financial Services",
    "CRAFTSMAN.NS": "Industrials",
    "EMIL.NS": "Consumer Discretionary",
    "GALAXYSURF.NS": "Basic Materials",
}


SECTOR_PHRASE_OVERRIDES: list[tuple[str, tuple[str, ...]]] = [
    ("Financial Services", (" ANGEL ONE ", " CARE RATINGS ", " ADITYA BIRLA MONEY ", " ADITYA BIRLA SUN LIFE AMC ")),
    ("Industrials", (" ABB INDIA ", " BHARAT ELECTRONICS ", " CRAFTSMAN AUTOMATION ", " 3M INDIA ")),
    ("Pharmaceuticals", (" ABBOTT INDIA ",)),
    ("Basic Materials", (" ACC LIMITED ", " AMBUJA CEMENTS ",)),
    ("Automobile", (" ASHOK LEYLAND ", " ATHER ENERGY ")),
    ("Real Estate", (" AURUM PROPTECH ",)),
    ("Consumer Staples", (" BAJAJ CONSUMER CARE ", " AVANTI FEEDS ", " AWL AGRI BUSINESS ")),
]


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
            " AMC ",
            " FINTECH",
            " FINTRADE",
            " FINANCIERS",
            " BROKING ",
            " MONEY ",
            " EXCHANGE ",
            " MICROFINANCE ",
        ),
    ),
    ("Telecom", (" TELECOM ", " COMMUNICATION", " BROADBAND ", " SATELLITE ", " NETWORK ", " NETWORKS ")),
    (
        "Utilities",
        (
            " POWER ",
            " ELECTRIC",
            " WATER ",
            " GRID ",
            " HYDRO",
            " SOLAR ",
            " WIND ",
            " RENEWABLE ",
            " GREEN ENERGY",
            " TRANSMISSION",
            " TRANSITION",
            " TRANSFORMER",
            " TRANSFORMERS",
            " HYDROGEN ",
        ),
    ),
    (
        "Energy",
        (
            " OIL ",
            " GAS ",
            " PETRO",
            " COAL ",
            " REFIN",
            " LNG ",
            " EXPLORATION ",
            " DRILLING ",
            " DRILL ",
            " OFFSHORE",
            " PIPELINE",
        ),
    ),
    (
        "Pharmaceuticals",
        (
            " PHARMA ",
            " PHARMACEUTICAL",
            " PHARMALABS",
            " DRUG",
            " LABS ",
            " LABORATORIES ",
            " BIOTECH",
            " BIOSCIENCE",
            " BIOSCIENCES",
            " THERAPEUT",
            " FORMULATION",
            " LIFESCI",
            " LIFE SCIENCES",
            " REMED",
            " ENZYME",
            " BLACKBIO",
        ),
    ),
    ("Healthcare", (" HEALTHCARE ", " HEALTH CARE ", " HOSPITAL", " MEDICAL ", " DIAGNOST", " CLINIC ", " HEALTH ", " MEDICARE ")),
    ("Automobile", (" AUTO ", " AUTOMOT", " MOTORS ", " MOTOR ", " TYRE ", " TIRES ", " TRACTOR", " MOBILITY ", " LEYLAND", " VEHICLE ")),
    (
        "Consumer Staples",
        (
            " FOOD",
            " FOODS",
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
            " CONSUMER CARE",
            " FEED ",
            " FEEDS",
            " AGRI BUSINESS",
            " NATURAL PRODUCTS",
            " PROTEINS",
        ),
    ),
    (
        "Consumer Discretionary",
        (
            " RETAIL",
            " APPAREL",
            " FASHION",
            " FASHIONS",
            " TEXTILE",
            " TEXTILES",
            " GARMENT",
            " GARMENTS",
            " FOOTWEAR",
            " JEWEL",
            " JEWELS",
            " LIFESTYLE",
            " HOTELS",
            " TRAVEL",
            " LEISURE",
            " RESTAUR",
            " FURNITURE",
            " DECOR ",
            " CYCLE",
            " CYCLES",
            " SPINTEX",
            " COTSPIN",
            " SYNTEX",
            " SPINNING",
            " COTTON",
            " FABRIC",
            " BRANDS",
        ),
    ),
    (
        "Real Estate",
        (
            " REALTY",
            " REAL ESTATE",
            " PROPERT",
            " ESTATE ",
            " REIT ",
            " LAND ",
            " LAND HOLDINGS",
            " SPACES",
            " REALTECH",
            " HOUSING ",
        ),
    ),
    (
        "Basic Materials",
        (
            " STEEL ",
            " METAL ",
            " METALS ",
            " MINING",
            " MINES ",
            " ALLOY",
            " CEMENT ",
            " CEMENTS",
            " CHEM",
            " FERT",
            " PAINT",
            " CERAM",
            " COPPER",
            " ALUMIN",
            " POLYMER",
            " PLASTIC",
            " MICRON",
            " PAPER",
            " PAPERS",
            " BOARD ",
            " BOARDS ",
            " GLASS ",
            " GRANITO",
            " TILES",
            " TILE ",
            " TUBE",
            " TUBES",
            " PIPE",
            " PIPES",
            " METCAST",
            " MINECHEM",
            " MINERAL",
            " MINERALS",
            " ORGANIC",
            " ORGANICS",
            " ALKALI",
            " LAMINATE",
            " LAMINATES",
            " COLOR",
            " COLOUR",
            " COLOURS",
            " RASAYAN",
            " PHOS ",
            " PHOSPH",
            " SUGAR",
            " SUGARS",
        ),
    ),
    (
        "Industrials",
        (
            " INFRA",
            " ENGINEER",
            " ENGINEERS",
            " LOGISTICS",
            " TERMINALS",
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
            " PROJECT ",
            " MACHIN",
            " INDUSTR",
            " EQUIP",
            " RAIL ",
            " RAILWAY",
            " MARINE ",
            " SOLONICS",
            " ENCON",
            " WELDING",
            " VALVES",
            " CASTALLOY",
            " HANDLING",
            " BUILDCON",
            " CONTRACTS",
            " CONTRACT ",
            " NIRMAN",
            " FOUNDATIONS",
            " SUPERSTRUCTURES",
            " PUMPS",
            " WIRES",
            " WIRE ",
            " ROBOTIC",
            " AUTOMATION",
            " MICROWAVE",
            " ELECTRONIC",
            " ELECTRONICS ",
            " AEROFLEX",
            " TOOLS",
        ),
    ),
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
            " KNOWLEDGEWARE",
            " ANALYTICS",
            " INFORMATICS",
            " OPTIFIBRE",
            " NEXUS ",
        ),
    ),
    ("Media", (" MEDIA ", " BROADCAST", " ENTERTAIN", " FILMS ", " MUSIC ", " TV ", " RADIO ")),
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
    normalized_symbol = str(symbol or "").strip().upper()
    if normalized_symbol and normalized_symbol in SECTOR_SYMBOL_OVERRIDES:
        return SECTOR_SYMBOL_OVERRIDES[normalized_symbol]

    haystack = _identity_text(company_name, symbol, *(aliases or []))
    for sector, keywords in SECTOR_PHRASE_OVERRIDES:
        if any(keyword in haystack for keyword in keywords):
            return sector
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
