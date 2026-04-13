from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def safe_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except Exception:
        return None


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def first_scalar(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def normalize_newlines(text: str) -> str:
    return text.replace('\r\n', '\n').replace('\r', '\n')


def iter_files(root: Path, suffixes: Iterable[str]) -> Iterable[Path]:
    wanted = {s.lower() for s in suffixes}
    for path in root.rglob('*'):
        if path.is_file() and path.suffix.lower() in wanted:
            yield path
