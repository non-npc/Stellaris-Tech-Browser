from __future__ import annotations

from pathlib import Path
from typing import Dict


def parse_localisation_file(path: Path, language: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    text = path.read_text(encoding='utf-8-sig', errors='replace')
    active_language = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        if line.endswith(':') and not '"' in line:
            active_language = line[:-1].strip().lower()
            continue
        if active_language and active_language != language.lower():
            continue
        if ':' not in line or '"' not in line:
            continue
        key, rest = line.split(':', 1)
        key = key.strip()
        first_quote = rest.find('"')
        last_quote = rest.rfind('"')
        if first_quote == -1 or last_quote <= first_quote:
            continue
        value = rest[first_quote + 1:last_quote]
        value = value.replace('\\n', '\n').replace('\\"', '"')
        result[key] = value
    return result
