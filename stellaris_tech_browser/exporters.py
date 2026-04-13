from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List


def export_json_from_db(db_path: Path, out_path: Path) -> Path:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    tech_rows = conn.execute(
        '''
        SELECT t.*, s.source_type, s.mod_name, s.root_path, f.relative_path
        FROM technologies t
        LEFT JOIN sources s ON t.source_id = s.id
        LEFT JOIN files f ON t.file_id = f.id
        ORDER BY t.area, t.tier, t.display_name
        '''
    ).fetchall()

    prereq_map: Dict[int, List[str]] = {}
    unlock_map: Dict[int, List[str]] = {}
    for row in conn.execute(
        '''
        SELECT tp.tech_id, tp.prerequisite_tech_id, t1.tech_key AS tech_key, t2.tech_key AS prereq_key
        FROM technology_prerequisites tp
        JOIN technologies t1 ON tp.tech_id = t1.id
        JOIN technologies t2 ON tp.prerequisite_tech_id = t2.id
        '''
    ):
        prereq_map.setdefault(row['tech_id'], []).append(row['prereq_key'])
        unlock_map.setdefault(row['prerequisite_tech_id'], []).append(row['tech_key'])

    unlockables_map: Dict[int, List[Dict[str, Any]]] = {}
    for row in conn.execute(
        '''
        SELECT tu.tech_id, u.unlock_key, u.display_name, u.unlock_type
        FROM technology_unlocks tu
        JOIN unlockables u ON tu.unlockable_id = u.id
        ORDER BY u.unlock_type, u.unlock_key
        '''
    ):
        unlockables_map.setdefault(row['tech_id'], []).append(
            {
                'unlock_key': row['unlock_key'],
                'display_name': row['display_name'],
                'unlock_type': row['unlock_type'],
            }
        )

    categories_map: Dict[int, List[str]] = {}
    for row in conn.execute('SELECT tech_id, category FROM technology_categories ORDER BY category'):
        categories_map.setdefault(row['tech_id'], []).append(row['category'])

    warnings = [dict(row) for row in conn.execute('SELECT severity, warning_type, message FROM warnings ORDER BY id')]

    techs = []
    edges = []
    for row in tech_rows:
        tech_id = row['id']
        for prereq_key in prereq_map.get(tech_id, []):
            edges.append({'from': prereq_key, 'to': row['tech_key'], 'type': 'prerequisite'})
        techs.append(
            {
                'id': row['tech_key'],
                'display_name': row['display_name'],
                'description': row['description'],
                'area': row['area'],
                'tier': row['tier'],
                'category': categories_map.get(tech_id, []),
                'cost_raw': row['cost_raw'],
                'cost_resolved': row['cost_resolved'],
                'levels': row['levels'],
                'is_repeatable': bool(row['is_repeatable']),
                'is_rare': bool(row['is_rare']),
                'is_dangerous': bool(row['is_dangerous']),
                'start_tech': bool(row['start_tech']),
                'gateway': row['gateway'],
                'ai_update_type': row['ai_update_type'],
                'prerequisites': prereq_map.get(tech_id, []),
                'unlocks_techs': unlock_map.get(tech_id, []),
                'unlocks_content': unlockables_map.get(tech_id, []),
                'defined_in': row['relative_path'],
                'source_type': row['source_type'],
                'source_mod': row['mod_name'],
                'root_path': row['root_path'],
                'raw_block_json': row['raw_block_json'],
            }
        )

    payload = {
        'meta': {
            'db_path': str(db_path),
            'technology_count': len(techs),
            'edge_count': len(edges),
        },
        'techs': techs,
        'edges': edges,
        'warnings': warnings,
    }

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    conn.close()
    return out_path
