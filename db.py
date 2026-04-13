from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Sequence

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    source_type TEXT NOT NULL,
    mod_name TEXT,
    root_path TEXT NOT NULL,
    load_order INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL,
    relative_path TEXT NOT NULL,
    absolute_path TEXT NOT NULL,
    file_type TEXT NOT NULL,
    checksum TEXT,
    last_modified TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS technologies (
    id INTEGER PRIMARY KEY,
    tech_key TEXT NOT NULL UNIQUE,
    display_name TEXT,
    description TEXT,
    area TEXT,
    tier INTEGER,
    cost_raw TEXT,
    cost_resolved REAL,
    levels INTEGER,
    cost_per_level_raw TEXT,
    cost_per_level_resolved REAL,
    is_repeatable INTEGER DEFAULT 0,
    is_rare INTEGER DEFAULT 0,
    is_dangerous INTEGER DEFAULT 0,
    start_tech INTEGER DEFAULT 0,
    gateway TEXT,
    ai_update_type TEXT,
    weight_raw TEXT,
    potential_raw TEXT,
    modifier_raw TEXT,
    weight_modifier_raw TEXT,
    icon TEXT,
    file_id INTEGER,
    source_id INTEGER,
    raw_block_json TEXT,
    active_version INTEGER DEFAULT 1,
    overridden_by_tech_id INTEGER,
    FOREIGN KEY (file_id) REFERENCES files(id),
    FOREIGN KEY (source_id) REFERENCES sources(id),
    FOREIGN KEY (overridden_by_tech_id) REFERENCES technologies(id)
);

CREATE TABLE IF NOT EXISTS technology_categories (
    id INTEGER PRIMARY KEY,
    tech_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    FOREIGN KEY (tech_id) REFERENCES technologies(id)
);

CREATE TABLE IF NOT EXISTS technology_prerequisites (
    id INTEGER PRIMARY KEY,
    tech_id INTEGER NOT NULL,
    prerequisite_tech_id INTEGER NOT NULL,
    FOREIGN KEY (tech_id) REFERENCES technologies(id),
    FOREIGN KEY (prerequisite_tech_id) REFERENCES technologies(id)
);

CREATE TABLE IF NOT EXISTS unlockables (
    id INTEGER PRIMARY KEY,
    unlock_key TEXT NOT NULL,
    display_name TEXT,
    unlock_type TEXT NOT NULL,
    file_id INTEGER,
    source_id INTEGER,
    raw_block_json TEXT,
    FOREIGN KEY (file_id) REFERENCES files(id),
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS technology_unlocks (
    id INTEGER PRIMARY KEY,
    tech_id INTEGER NOT NULL,
    unlockable_id INTEGER NOT NULL,
    relation_type TEXT DEFAULT 'direct',
    FOREIGN KEY (tech_id) REFERENCES technologies(id),
    FOREIGN KEY (unlockable_id) REFERENCES unlockables(id)
);

CREATE TABLE IF NOT EXISTS localisation (
    id INTEGER PRIMARY KEY,
    loc_key TEXT NOT NULL,
    language TEXT NOT NULL,
    text_value TEXT NOT NULL,
    file_id INTEGER,
    source_id INTEGER,
    FOREIGN KEY (file_id) REFERENCES files(id),
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS scan_runs (
    id INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    game_path TEXT NOT NULL,
    mods_path TEXT,
    language TEXT NOT NULL,
    technology_count INTEGER DEFAULT 0,
    unlockable_count INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warnings (
    id INTEGER PRIMARY KEY,
    scan_run_id INTEGER NOT NULL,
    severity TEXT NOT NULL,
    warning_type TEXT NOT NULL,
    message TEXT NOT NULL,
    file_id INTEGER,
    tech_id INTEGER,
    FOREIGN KEY (scan_run_id) REFERENCES scan_runs(id),
    FOREIGN KEY (file_id) REFERENCES files(id),
    FOREIGN KEY (tech_id) REFERENCES technologies(id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS technology_search USING fts5(
    tech_key,
    display_name,
    description,
    content=''
);

CREATE INDEX IF NOT EXISTS idx_technologies_key ON technologies(tech_key);
CREATE INDEX IF NOT EXISTS idx_technologies_area ON technologies(area);
CREATE INDEX IF NOT EXISTS idx_technologies_tier ON technologies(tier);
CREATE INDEX IF NOT EXISTS idx_categories_tech_id ON technology_categories(tech_id);
CREATE INDEX IF NOT EXISTS idx_categories_category ON technology_categories(category);
CREATE INDEX IF NOT EXISTS idx_prereq_tech_id ON technology_prerequisites(tech_id);
CREATE INDEX IF NOT EXISTS idx_prereq_prereq_id ON technology_prerequisites(prerequisite_tech_id);
CREATE INDEX IF NOT EXISTS idx_unlocks_tech_id ON technology_unlocks(tech_id);
CREATE INDEX IF NOT EXISTS idx_localisation_key_lang ON localisation(loc_key, language);
"""


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def initialize_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def reset_content(conn: sqlite3.Connection) -> None:
    tables = [
        'technology_search',
        'technology_unlocks',
        'technology_prerequisites',
        'technology_categories',
        'unlockables',
        'technologies',
        'localisation',
        'warnings',
        'files',
        'sources',
        'scan_runs',
    ]
    for table in tables:
        conn.execute(f'DELETE FROM {table}')
    conn.commit()


def executemany(conn: sqlite3.Connection, sql: str, rows: Iterable[Sequence]) -> None:
    conn.executemany(sql, rows)


def fetch_all(conn: sqlite3.Connection, sql: str, params: Sequence | None = None):
    cur = conn.execute(sql, params or [])
    return cur.fetchall()
