"""
Migration scoring and classification functions for ADF to Fabric Migration Tool
"""

from typing import Iterable

from constants import (
    CONTROL_ACTIVITY_TYPES,
    CONNECTIVITY_COMPLEX_KEYWORDS,
    SUPPORTED_MIGRATABLE,
)
from utilities import _normalize_type


def score_component_parity(total_acts: int, non_migratable: int) -> int:
    """Score component parity based on migratable activities."""
    if total_acts <= 0 or non_migratable <= 0:
        return 0
    ratio = non_migratable / max(total_acts, 1)
    if non_migratable <= 2 and ratio <= 0.1:
        return 1
    if non_migratable <= 5 and ratio <= 0.3:
        return 2
    return 3


def score_non_migratable(non_migratable: int) -> int:
    """Score based on number of non-migratable components."""
    if non_migratable <= 0:
        return 0
    if non_migratable <= 2:
        return 1
    if non_migratable <= 5:
        return 2
    return 3


def score_connectivity(ls_types: Iterable[str]) -> int:
    """Score connectivity complexity based on linked service types."""
    types = [t.lower() for t in ls_types if t]
    if not types:
        return 0
    flagged = sum(1 for t in types if any(k in t for k in CONNECTIVITY_COMPLEX_KEYWORDS))
    if flagged == 0:
        return 0
    if flagged <= 1:
        return 1
    if flagged <= 3:
        return 2
    return 3


def score_orchestration(total_acts: int, control_acts: int) -> int:
    """Score orchestration complexity based on control activities."""
    if total_acts <= 5 and control_acts == 0:
        return 0
    if total_acts <= 10 and control_acts <= 1:
        return 1
    if total_acts <= 20 or control_acts <= 3:
        return 2
    return 3


def is_migratable(activity_type: str) -> bool:
    """Check if an activity type is migratable to Fabric."""
    return _normalize_type(activity_type) in SUPPORTED_MIGRATABLE


def get_activity_category(activity_type: str) -> str:
    """Categorize an activity by type."""
    norm = _normalize_type(activity_type)
    if not norm:
        return "Other"

    direct_map = {
        "copy": "Move & Transform",
        "executepipeline": "Orchestration",
        "ifcondition": "Orchestration",
        "wait": "Orchestration",
        "web": "External Service",
        "setvariable": "Orchestration",
        "azurefunction": "Compute",
        "foreach": "Orchestration",
        "lookup": "Data Lookup",
        "switch": "Orchestration",
        "sqlserverstoredprocedure": "Database",
        "notebook": "Synapse Notebook",
        "executedataflow": "General"
    }

    if norm in direct_map:
        return direct_map[norm]

    if "databricks" in norm and "notebook" in norm:
        return "Databricks Notebook"
    if "synapse" in norm and "notebook" in norm:
        return "Synapse Notebook"
    if "notebook" in norm:
        return "Notebook"
    if "copy" in norm:
        return "Move & Transform"

    return "Other"
