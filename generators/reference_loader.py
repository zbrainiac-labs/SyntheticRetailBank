"""
Reference / configuration data shared by the LCR and DtI generators and the
post_deploy.sql seeds (single source of truth: reference_data/liqi/*.csv).
Only reference data lives there; every dated or event row is generated.
"""

from __future__ import annotations

import csv
import json
from datetime import date
from functools import cache
from pathlib import Path

REF_DIR = Path(__file__).resolve().parent.parent / "reference_data" / "liqi"
SCENARIO_DIR = Path(__file__).resolve().parent / "scenarios"


@cache
def _rows(name: str) -> tuple[dict, ...]:
    with open(REF_DIR / f"{name}.csv", newline="") as f:
        return tuple(csv.DictReader(f))


def _bool(v: str) -> bool:
    return v == "TRUE"


def holidays() -> set:
    """Zurich bank holidays (LIQI_RAW_TB_DTI_HOLIDAYS)."""
    return {date.fromisoformat(r["HOLIDAY_DATE"]) for r in _rows("dti_holidays")}


def hqla_types() -> dict[str, tuple[str, float]]:
    """asset_type -> (regulatory level, haircut factor) (LIQI_RAW_TB_HQLA_ELIGIBILITY)."""
    return {r["ASSET_TYPE"]: (r["REGULATORY_LEVEL"], float(r["HAIRCUT_FACTOR"])) for r in _rows("hqla_eligibility")}


def deposit_types() -> dict[str, tuple[str, float, bool, bool]]:
    """deposit_type -> (counterparty type, base run-off, allows relationship discount, is operational)."""
    return {r["DEPOSIT_TYPE"]: (r["COUNTERPARTY_TYPE"], float(r["BASE_RUN_OFF_RATE"]),
                                _bool(r["ALLOWS_RELATIONSHIP_DISCOUNT"]), _bool(r["IS_OPERATIONAL"]))
            for r in _rows("deposit_types")}


def contract_rules() -> dict[tuple[str, str], tuple[str, float]]:
    """(contract product, sector) -> (ASSET | FACILITY | LIABILITY, BASE rate) from the BASE behavioral rules."""
    out = {}
    for r in _rows("behavioral_rules"):
        if r["STRESS_SCENARIO"] != "BASE" or r["COUNTERPARTY_SECTOR"] == "ALL":
            continue
        if r["INFLOW_RATE"]:
            out[(r["PRODUCT_TYPE"], r["COUNTERPARTY_SECTOR"])] = ("ASSET", float(r["INFLOW_RATE"]))
        elif r["DRAWDOWN_RATE"]:
            out[(r["PRODUCT_TYPE"], r["COUNTERPARTY_SECTOR"])] = ("FACILITY", float(r["DRAWDOWN_RATE"]))
        elif r["PRODUCT_TYPE"] == "INTERBANK_BORROWING":
            out[(r["PRODUCT_TYPE"], r["COUNTERPARTY_SECTOR"])] = ("LIABILITY", float(r["RUNOFF_RATE"]))
    return out


def wlk_clusters() -> dict[str, tuple[str, float]]:
    """WLK cluster -> (HQLA category, haircut) (LIQI_RAW_TB_DTI_WLK_CLUSTERS)."""
    return {r["WLK_CLUSTER"]: (r["HQLA_CATEGORY"], float(r["HAIRCUT"])) for r in _rows("dti_wlk_clusters")}


def entity_shares() -> list[tuple[str, int]]:
    """[(entity, weight in percent)] in display order (LIQI_RAW_TB_DTI_ENTITIES.CB_BALANCE_SHARE)."""
    rows = sorted(_rows("dti_entities"), key=lambda r: int(r["SORT_ORDER"]))
    return [(r["ENTITY_ID"], round(float(r["CB_BALANCE_SHARE"]) * 100)) for r in rows]


def scenario(name: str = "lcr_dti_showcase") -> dict:
    """Showcase event configuration (offsets, sizes); the event rows themselves are generated."""
    return json.loads((SCENARIO_DIR / f"{name}.json").read_text())
