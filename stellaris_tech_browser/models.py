from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class SourceInfo:
    source_type: str  # vanilla or mod
    mod_name: Optional[str]
    root_path: Path
    load_order: int


@dataclass
class ParsedTech:
    tech_key: str
    data: Dict[str, Any]
    source: SourceInfo
    relative_path: str
    absolute_path: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    area: Optional[str] = None
    tier: Optional[int] = None
    cost_raw: Optional[str] = None
    cost_resolved: Optional[float] = None
    levels: Optional[int] = None
    cost_per_level_raw: Optional[str] = None
    cost_per_level_resolved: Optional[float] = None
    is_repeatable: bool = False
    is_rare: bool = False
    is_dangerous: bool = False
    start_tech: bool = False
    gateway: Optional[str] = None
    ai_update_type: Optional[str] = None
    weight_raw: Optional[str] = None
    potential_raw: Optional[str] = None
    modifier_raw: Optional[str] = None
    weight_modifier_raw: Optional[str] = None
    icon: Optional[str] = None
    categories: List[str] = field(default_factory=list)
    prerequisites: List[str] = field(default_factory=list)
    raw_block_json: Optional[str] = None


@dataclass
class ParsedUnlockable:
    unlock_key: str
    unlock_type: str
    source: SourceInfo
    relative_path: str
    absolute_path: str
    data: Dict[str, Any]
    display_name: Optional[str] = None
    raw_block_json: Optional[str] = None
    referenced_tech_keys: List[str] = field(default_factory=list)


@dataclass
class ScanWarning:
    severity: str
    warning_type: str
    message: str
    relative_path: Optional[str] = None
    tech_key: Optional[str] = None
