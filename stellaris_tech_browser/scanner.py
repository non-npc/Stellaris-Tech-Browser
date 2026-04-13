from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from .clausewitz_parser import ClausewitzParser, ClausewitzParserError
from .db import connect_db, initialize_db, reset_content
from .localisation import parse_localisation_file
from .models import ParsedTech, ParsedUnlockable, ScanWarning, SourceInfo
from .utils import as_list, first_scalar, iter_files, safe_float, safe_int, to_json

ProgressCallback = Callable[[int, str, str], None]

UNLOCKABLE_DIRS: dict[str, str] = {
    'common/buildings': 'building',
    'common/component_templates': 'component',
    'common/starbase_buildings': 'starbase_building',
    'common/starbase_modules': 'starbase_module',
    'common/edicts': 'edict',
    'common/policies': 'policy',
    'common/armies': 'army',
    'common/megastructures': 'megastructure',
    'common/ship_sizes': 'ship_size',
    'common/sections': 'section',
    'common/ascension_perks': 'ascension_perk',
    'common/agreement_terms': 'agreement_term',
    'common/deposits': 'deposit',
    'common/situations': 'situation',
}


class StellarisTechScanner:
    def __init__(self, game_path: Path, mods_path: Optional[Path], db_path: Path, language: str = 'english') -> None:
        self.game_path = game_path
        self.mods_path = mods_path
        self.db_path = db_path
        self.language = language.lower()
        self.parser = ClausewitzParser()
        self.warnings: List[ScanWarning] = []

    def scan(self, progress: Optional[ProgressCallback] = None) -> Path:
        def emit(percent: int, phase: str, detail: str = '') -> None:
            if progress:
                progress(percent, phase, detail)

        emit(1, 'Preparing', 'Initializing database')
        conn = connect_db(self.db_path)
        initialize_db(conn)
        reset_content(conn)

        started_at = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
        scan_run_id = conn.execute(
            'INSERT INTO scan_runs(started_at, game_path, mods_path, language, status) VALUES (?, ?, ?, ?, ?)',
            (started_at, str(self.game_path), str(self.mods_path) if self.mods_path else None, self.language, 'running'),
        ).lastrowid

        sources = self._collect_sources()
        emit(5, 'Discovering Sources', f'{len(sources)} source root(s)')
        source_id_map = self._insert_sources(conn, sources)

        loc_map: Dict[str, str] = {}
        techs: Dict[str, ParsedTech] = {}
        unlockables: List[ParsedUnlockable] = []
        file_cache: Dict[Tuple[int, str], int] = {}

        total_roots = max(len(sources), 1)
        for index, source in enumerate(sources):
            base_pct = 10 + int((index / total_roots) * 70)
            emit(base_pct, 'Scanning Technologies', source.mod_name or 'vanilla')
            self._scan_technologies_for_source(conn, source, source_id_map[source.root_path], techs, file_cache)
            emit(base_pct + 10, 'Scanning Unlockables', source.mod_name or 'vanilla')
            self._scan_unlockables_for_source(conn, source, source_id_map[source.root_path], unlockables, file_cache)
            emit(base_pct + 15, 'Scanning Localisation', source.mod_name or 'vanilla')
            self._scan_localisation_for_source(conn, source, source_id_map[source.root_path], loc_map, file_cache)

        emit(82, 'Resolving Localisation', f'{len(loc_map)} strings loaded')
        for tech in techs.values():
            tech.display_name = loc_map.get(tech.tech_key) or tech.tech_key
            tech.description = loc_map.get(f'{tech.tech_key}_desc') or ''

        emit(86, 'Writing Technologies', f'{len(techs)} records')
        tech_id_map = self._insert_techs(conn, techs)
        emit(90, 'Writing Graph Edges', 'Prerequisites and reverse unlocks')
        self._insert_prerequisites(conn, techs, tech_id_map)
        emit(93, 'Writing Unlockables', f'{len(unlockables)} records')
        self._insert_unlockables(conn, unlockables, tech_id_map)
        emit(96, 'Writing Localisation', f'{len(loc_map)} rows')
        self._insert_localisation(conn, loc_map)
        emit(98, 'Writing Warnings', f'{len(self.warnings)} warning(s)')
        self._insert_warnings(conn, scan_run_id)

        conn.execute(
            'UPDATE scan_runs SET finished_at=?, technology_count=?, unlockable_count=?, warning_count=?, status=? WHERE id=?',
            (
                datetime.utcnow().isoformat(timespec='seconds') + 'Z',
                len(techs),
                len(unlockables),
                len(self.warnings),
                'completed',
                scan_run_id,
            ),
        )
        conn.commit()
        conn.close()
        emit(100, 'Complete', str(self.db_path))
        return self.db_path

    def _collect_sources(self) -> List[SourceInfo]:
        sources = [SourceInfo('vanilla', None, self.game_path, 0)]
        if self.mods_path and self.mods_path.exists():
            mod_dirs = []
            for path in self.mods_path.iterdir():
                if not path.is_dir():
                    continue
                if (path / 'common').exists() or (path / 'descriptor.mod').exists():
                    mod_dirs.append(path)
            mod_dirs.sort(key=lambda p: p.name.lower())
            for idx, path in enumerate(mod_dirs, start=1):
                sources.append(SourceInfo('mod', path.name, path, idx))
        return sources

    def _insert_sources(self, conn: sqlite3.Connection, sources: List[SourceInfo]) -> Dict[Path, int]:
        mapping: Dict[Path, int] = {}
        for source in sources:
            source_id = conn.execute(
                'INSERT INTO sources(source_type, mod_name, root_path, load_order) VALUES (?, ?, ?, ?)',
                (source.source_type, source.mod_name, str(source.root_path), source.load_order),
            ).lastrowid
            mapping[source.root_path] = int(source_id)
        conn.commit()
        return mapping

    def _register_file(self, conn: sqlite3.Connection, source_id: int, path: Path, root: Path, file_type: str, file_cache: Dict[Tuple[int, str], int]) -> int:
        rel = str(path.relative_to(root)).replace('\\', '/')
        key = (source_id, rel)
        if key in file_cache:
            return file_cache[key]
        checksum = self._sha1(path)
        stat = path.stat()
        file_id = conn.execute(
            'INSERT INTO files(source_id, relative_path, absolute_path, file_type, checksum, last_modified) VALUES (?, ?, ?, ?, ?, ?)',
            (source_id, rel, str(path), file_type, checksum, datetime.utcfromtimestamp(stat.st_mtime).isoformat(timespec='seconds') + 'Z'),
        ).lastrowid
        file_cache[key] = int(file_id)
        return int(file_id)

    def _scan_technologies_for_source(self, conn: sqlite3.Connection, source: SourceInfo, source_id: int, techs: Dict[str, ParsedTech], file_cache: Dict[Tuple[int, str], int]) -> None:
        tech_root = source.root_path / 'common' / 'technology'
        if not tech_root.exists():
            return
        for path in iter_files(tech_root, ['.txt']):
            try:
                file_id = self._register_file(conn, source_id, path, source.root_path, 'technology', file_cache)
                parsed = self.parser.parse_file(path)
                rel = str(path.relative_to(source.root_path)).replace('\\', '/')
                for tech_key, block in parsed.items():
                    if not isinstance(block, dict):
                        continue
                    if tech_key in techs:
                        self.warnings.append(ScanWarning('warning', 'duplicate_tech_id', f'{tech_key} overridden by {source.mod_name or "vanilla"}', rel, tech_key))
                    techs[tech_key] = self._build_parsed_tech(tech_key, block, source, rel, str(path))
            except (ClausewitzParserError, OSError) as exc:
                self.warnings.append(ScanWarning('error', 'parse_error', str(exc), str(path.relative_to(source.root_path)).replace('\\', '/')))

    def _build_parsed_tech(self, tech_key: str, block: Dict[str, Any], source: SourceInfo, relative_path: str, absolute_path: str) -> ParsedTech:
        categories = [str(x) for x in as_list(block.get('category')) if x is not None]
        prerequisites = [str(x) for x in as_list(block.get('prerequisites')) if isinstance(x, str)]
        if len(prerequisites) == 1 and isinstance(block.get('prerequisites'), dict):
            prerequisites = [str(x) for x in as_list(block['prerequisites'].get('__items__'))]
        return ParsedTech(
            tech_key=tech_key,
            data=block,
            source=source,
            relative_path=relative_path,
            absolute_path=absolute_path,
            area=str(first_scalar(block.get('area')) or '') or None,
            tier=safe_int(first_scalar(block.get('tier'))),
            cost_raw=str(first_scalar(block.get('cost'))) if block.get('cost') is not None else None,
            cost_resolved=safe_float(first_scalar(block.get('cost'))),
            levels=safe_int(first_scalar(block.get('levels'))),
            cost_per_level_raw=str(first_scalar(block.get('cost_per_level'))) if block.get('cost_per_level') is not None else None,
            cost_per_level_resolved=safe_float(first_scalar(block.get('cost_per_level'))),
            is_repeatable=self._to_bool(first_scalar(block.get('is_repeatable'))),
            is_rare=self._to_bool(first_scalar(block.get('is_rare'))),
            is_dangerous=self._to_bool(first_scalar(block.get('is_dangerous'))),
            start_tech=self._to_bool(first_scalar(block.get('start_tech'))),
            gateway=str(first_scalar(block.get('gateway'))) if block.get('gateway') is not None else None,
            ai_update_type=str(first_scalar(block.get('ai_update_type'))) if block.get('ai_update_type') is not None else None,
            weight_raw=to_json(block.get('weight')) if block.get('weight') is not None else None,
            potential_raw=to_json(block.get('potential')) if block.get('potential') is not None else None,
            modifier_raw=to_json(block.get('modifier')) if block.get('modifier') is not None else None,
            weight_modifier_raw=to_json(block.get('weight_modifier')) if block.get('weight_modifier') is not None else None,
            icon=str(first_scalar(block.get('icon'))) if block.get('icon') is not None else None,
            categories=categories,
            prerequisites=prerequisites,
            raw_block_json=to_json(block),
        )

    def _scan_unlockables_for_source(self, conn: sqlite3.Connection, source: SourceInfo, source_id: int, unlockables: List[ParsedUnlockable], file_cache: Dict[Tuple[int, str], int]) -> None:
        tech_token_cache: set[str] = set()
        # We capture a broad list of likely tech-like tokens later when linking.
        for rel_dir, unlock_type in UNLOCKABLE_DIRS.items():
            root = source.root_path / Path(rel_dir)
            if not root.exists():
                continue
            for path in iter_files(root, ['.txt']):
                try:
                    self._register_file(conn, source_id, path, source.root_path, unlock_type, file_cache)
                    parsed = self.parser.parse_file(path)
                    rel = str(path.relative_to(source.root_path)).replace('\\', '/')
                    for unlock_key, block in parsed.items():
                        if not isinstance(block, dict):
                            continue
                        refs = sorted(self._collect_tech_references(block))
                        unlockables.append(
                            ParsedUnlockable(
                                unlock_key=unlock_key,
                                unlock_type=unlock_type,
                                source=source,
                                relative_path=rel,
                                absolute_path=str(path),
                                data=block,
                                raw_block_json=to_json(block),
                                referenced_tech_keys=refs,
                            )
                        )
                        tech_token_cache.update(refs)
                except (ClausewitzParserError, OSError) as exc:
                    self.warnings.append(ScanWarning('warning', 'unlockable_parse_error', str(exc), str(path.relative_to(source.root_path)).replace('\\', '/')))

    def _scan_localisation_for_source(self, conn: sqlite3.Connection, source: SourceInfo, source_id: int, loc_map: Dict[str, str], file_cache: Dict[Tuple[int, str], int]) -> None:
        loc_root = source.root_path / 'localisation'
        if not loc_root.exists():
            return
        for path in iter_files(loc_root, ['.yml', '.yaml']):
            rel = str(path.relative_to(source.root_path)).replace('\\', '/')
            try:
                self._register_file(conn, source_id, path, source.root_path, 'localisation', file_cache)
                values = parse_localisation_file(path, self.language)
                loc_map.update(values)
            except OSError as exc:
                self.warnings.append(ScanWarning('warning', 'localisation_error', str(exc), rel))

    def _insert_techs(self, conn: sqlite3.Connection, techs: Dict[str, ParsedTech]) -> Dict[str, int]:
        tech_id_map: Dict[str, int] = {}
        file_lookup = self._make_file_lookup(conn)
        source_lookup = self._make_source_lookup(conn)
        for tech in techs.values():
            file_id = file_lookup.get((str(tech.source.root_path), tech.relative_path))
            source_id = source_lookup.get(str(tech.source.root_path))
            tech_id = conn.execute(
                '''
                INSERT INTO technologies(
                    tech_key, display_name, description, area, tier, cost_raw, cost_resolved, levels,
                    cost_per_level_raw, cost_per_level_resolved, is_repeatable, is_rare, is_dangerous,
                    start_tech, gateway, ai_update_type, weight_raw, potential_raw, modifier_raw,
                    weight_modifier_raw, icon, file_id, source_id, raw_block_json, active_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ''',
                (
                    tech.tech_key,
                    tech.display_name,
                    tech.description,
                    tech.area,
                    tech.tier,
                    tech.cost_raw,
                    tech.cost_resolved,
                    tech.levels,
                    tech.cost_per_level_raw,
                    tech.cost_per_level_resolved,
                    int(tech.is_repeatable),
                    int(tech.is_rare),
                    int(tech.is_dangerous),
                    int(tech.start_tech),
                    tech.gateway,
                    tech.ai_update_type,
                    tech.weight_raw,
                    tech.potential_raw,
                    tech.modifier_raw,
                    tech.weight_modifier_raw,
                    tech.icon,
                    file_id,
                    source_id,
                    tech.raw_block_json,
                ),
            ).lastrowid
            tech_id = int(tech_id)
            tech_id_map[tech.tech_key] = tech_id
            conn.executemany(
                'INSERT INTO technology_categories(tech_id, category) VALUES (?, ?)',
                [(tech_id, cat) for cat in tech.categories],
            )
            conn.execute(
                'INSERT INTO technology_search(rowid, tech_key, display_name, description) VALUES (?, ?, ?, ?)',
                (tech_id, tech.tech_key, tech.display_name or '', tech.description or ''),
            )
        conn.commit()
        return tech_id_map

    def _insert_prerequisites(self, conn: sqlite3.Connection, techs: Dict[str, ParsedTech], tech_id_map: Dict[str, int]) -> None:
        rows = []
        for tech in techs.values():
            tech_id = tech_id_map.get(tech.tech_key)
            if not tech_id:
                continue
            for prereq_key in tech.prerequisites:
                prereq_id = tech_id_map.get(prereq_key)
                if prereq_id:
                    rows.append((tech_id, prereq_id))
                else:
                    self.warnings.append(ScanWarning('warning', 'missing_prerequisite', f'{tech.tech_key} references missing prerequisite {prereq_key}', tech.relative_path, tech.tech_key))
        conn.executemany('INSERT INTO technology_prerequisites(tech_id, prerequisite_tech_id) VALUES (?, ?)', rows)
        conn.commit()

    def _insert_unlockables(self, conn: sqlite3.Connection, unlockables: List[ParsedUnlockable], tech_id_map: Dict[str, int]) -> None:
        file_lookup = self._make_file_lookup(conn)
        source_lookup = self._make_source_lookup(conn)
        unlock_rows = []
        for item in unlockables:
            file_id = file_lookup.get((str(item.source.root_path), item.relative_path))
            source_id = source_lookup.get(str(item.source.root_path))
            unlockable_id = conn.execute(
                'INSERT INTO unlockables(unlock_key, display_name, unlock_type, file_id, source_id, raw_block_json) VALUES (?, ?, ?, ?, ?, ?)',
                (item.unlock_key, item.display_name or item.unlock_key, item.unlock_type, file_id, source_id, item.raw_block_json),
            ).lastrowid
            for tech_key in item.referenced_tech_keys:
                tech_id = tech_id_map.get(tech_key)
                if tech_id:
                    unlock_rows.append((tech_id, int(unlockable_id), 'direct'))
        conn.executemany('INSERT INTO technology_unlocks(tech_id, unlockable_id, relation_type) VALUES (?, ?, ?)', unlock_rows)
        conn.commit()

    def _insert_localisation(self, conn: sqlite3.Connection, loc_map: Dict[str, str]) -> None:
        conn.executemany(
            'INSERT INTO localisation(loc_key, language, text_value) VALUES (?, ?, ?)',
            [(k, self.language, v) for k, v in loc_map.items()],
        )
        conn.commit()

    def _insert_warnings(self, conn: sqlite3.Connection, scan_run_id: int) -> None:
        conn.executemany(
            'INSERT INTO warnings(scan_run_id, severity, warning_type, message) VALUES (?, ?, ?, ?)',
            [(scan_run_id, w.severity, w.warning_type, w.message) for w in self.warnings],
        )
        conn.commit()

    def _make_file_lookup(self, conn: sqlite3.Connection) -> Dict[Tuple[str, str], int]:
        rows = conn.execute(
            'SELECT files.id, files.relative_path, sources.root_path FROM files JOIN sources ON files.source_id = sources.id'
        ).fetchall()
        return {(row['root_path'], row['relative_path']): row['id'] for row in rows}

    def _make_source_lookup(self, conn: sqlite3.Connection) -> Dict[str, int]:
        rows = conn.execute('SELECT id, root_path FROM sources').fetchall()
        return {row['root_path']: row['id'] for row in rows}

    def _collect_tech_references(self, value: Any) -> set[str]:
        found: set[str] = set()
        if isinstance(value, dict):
            for key, sub in value.items():
                if isinstance(key, str) and key.startswith('tech_'):
                    found.add(key)
                found.update(self._collect_tech_references(sub))
        elif isinstance(value, list):
            for sub in value:
                found.update(self._collect_tech_references(sub))
        elif isinstance(value, str):
            if value.startswith('tech_'):
                found.add(value)
        return found

    def _to_bool(self, value: Any) -> bool:
        if value is None:
            return False
        return str(value).strip().lower() in {'yes', 'true', '1'}

    def _sha1(self, path: Path) -> str:
        hasher = hashlib.sha1()
        with path.open('rb') as fh:
            for chunk in iter(lambda: fh.read(65536), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
