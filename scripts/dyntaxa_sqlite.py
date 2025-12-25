#!/usr/bin/env python3

# MIT License
#
# Copyright (c) 2025 Jonas Waldeck
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

DB_PATH_DEFAULT = Path("./tmp/dyntaxa_lepidoptera.sqlite")

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS taxa (
  taxon_id     INTEGER PRIMARY KEY,
  local_index  INTEGER UNIQUE NOT NULL,
  sci_name     TEXT,
  swe_name     TEXT,
  category     TEXT,
  type         TEXT,
  status       TEXT,
  parent_id    INTEGER,
  is_active    INTEGER NOT NULL DEFAULT 1,
  sha256       TEXT,
  updated_at   INTEGER NOT NULL,
  raw_json     TEXT
);

CREATE INDEX IF NOT EXISTS idx_taxa_active ON taxa(is_active);
CREATE INDEX IF NOT EXISTS idx_taxa_category ON taxa(category);
CREATE INDEX IF NOT EXISTS idx_taxa_sciname ON taxa(sci_name);

CREATE TABLE IF NOT EXISTS runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  lepidoptera_taxon_id INTEGER,
  child_ids_count INTEGER,

  -- NEW: source revision for this run
  source_hash TEXT,

  species_count INTEGER,
  inserted_count INTEGER,
  updated_count INTEGER,
  unchanged_count INTEGER,
  deactivated_count INTEGER
);

CREATE TABLE IF NOT EXISTS changes (
  change_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  taxon_id INTEGER NOT NULL,
  change_type TEXT NOT NULL,      -- inserted, updated, deactivated, reactivated
  old_sha256 TEXT,
  new_sha256 TEXT,
  at INTEGER NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
"""

def _now() -> int:
    return int(time.time())

def db_open(db_path: Path | None = None) -> sqlite3.Connection:
    db_path = db_path or DB_PATH_DEFAULT
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_SQL)

    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('schema_version','2')")
    cur.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('next_local_index','0')")
    # NEW: store last source hash (optional convenience)
    cur.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('last_source_hash','')")
    con.commit()
    return con

def _meta_get(con: sqlite3.Connection, key: str) -> str:
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    if not row:
        raise RuntimeError(f"Missing meta key: {key}")
    return str(row["value"])

def _meta_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO meta(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )

def alloc_local_index(con: sqlite3.Connection) -> int:
    con.execute("BEGIN IMMEDIATE")
    next_idx = int(_meta_get(con, "next_local_index"))
    _meta_set(con, "next_local_index", str(next_idx + 1))
    con.commit()
    return next_idx

def begin_run(con: sqlite3.Connection, lepidoptera_taxon_id: int, child_ids_count: int, source_hash: str | None = None) -> int:
    cur = con.execute(
        "INSERT INTO runs(started_at, lepidoptera_taxon_id, child_ids_count, source_hash) VALUES(?,?,?,?)",
        (_now(), lepidoptera_taxon_id, child_ids_count, source_hash),
    )
    # keep meta updated as convenience (not required)
    if source_hash is not None:
        _meta_set(con, "last_source_hash", source_hash)
    con.commit()
    return int(cur.lastrowid)

def end_run(
    con: sqlite3.Connection,
    run_id: int,
    *,
    species_count: int,
    inserted: int,
    updated: int,
    unchanged: int,
    deactivated: int
) -> None:
    con.execute(
        """
        UPDATE runs
        SET finished_at=?, species_count=?, inserted_count=?, updated_count=?, unchanged_count=?, deactivated_count=?
        WHERE run_id=?
        """,
        (_now(), species_count, inserted, updated, unchanged, deactivated, run_id),
    )
    con.commit()

def _pick_names_from_taxon_obj(taxon_obj: dict) -> tuple[str | None, str | None]:
    # taxonservice POST /taxa returnerar fältet "names": [...]
    sci = None
    swe = None
    for n in taxon_obj.get("names") or []:
        cat = (n.get("category") or {}).get("value")
        name = n.get("name")
        if not name:
            continue
        # ta första bästa; upstream verkar returnera recommended först, men vi är robusta
        if cat == "ScientificName" and sci is None:
            sci = name
        if cat == "SwedishName" and swe is None:
            swe = name
    return sci, swe

def get_taxon_sha(con: sqlite3.Connection, taxon_id: int) -> str | None:
    row = con.execute("SELECT sha256 FROM taxa WHERE taxon_id=?", (taxon_id,)).fetchone()
    return str(row["sha256"]) if row and row["sha256"] is not None else None

def upsert_taxon(
    con: sqlite3.Connection,
    run_id: int,
    taxon_obj: dict,
    sha256: str | None,
    *,
    make_active: bool = True
) -> str:
    """
    Returnerar change_type: inserted/updated/unchanged/reactivated
    Idempotent: om sha är samma och redan aktiv => ingen write.
    """
    taxon_id = int(taxon_obj.get("taxonId"))
    parent_id = taxon_obj.get("parentId")
    category = (taxon_obj.get("category") or {}).get("value")
    ttype = (taxon_obj.get("type") or {}).get("value")
    status = (taxon_obj.get("status") or {}).get("value")
    sci, swe = _pick_names_from_taxon_obj(taxon_obj)

    now = _now()
    raw_json = json.dumps(taxon_obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    row = con.execute(
        "SELECT taxon_id, local_index, sha256, is_active FROM taxa WHERE taxon_id=?",
        (taxon_id,),
    ).fetchone()

    if row is None:
        local_index = alloc_local_index(con)
        con.execute(
            """
            INSERT INTO taxa(taxon_id, local_index, sci_name, swe_name, category, type, status, parent_id, is_active, sha256, updated_at, raw_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (taxon_id, local_index, sci, swe, category, ttype, status, parent_id, 1 if make_active else 0, sha256, now, raw_json),
        )
        con.execute(
            "INSERT INTO changes(run_id,taxon_id,change_type,old_sha256,new_sha256,at) VALUES(?,?,?,?,?,?)",
            (run_id, taxon_id, "inserted", None, sha256, now),
        )
        con.commit()
        return "inserted"

    old_sha = row["sha256"]
    old_active = int(row["is_active"])

    # if unchanged and already active => no-op
    if make_active and old_active == 1 and sha256 is not None and old_sha == sha256:
        return "unchanged"

    # reactivation (was inactive)
    if make_active and old_active == 0:
        con.execute(
            """
            UPDATE taxa
            SET sci_name=?, swe_name=?, category=?, type=?, status=?, parent_id=?, is_active=1, sha256=?, updated_at=?, raw_json=?
            WHERE taxon_id=?
            """,
            (sci, swe, category, ttype, status, parent_id, sha256, now, raw_json, taxon_id),
        )
        con.execute(
            "INSERT INTO changes(run_id,taxon_id,change_type,old_sha256,new_sha256,at) VALUES(?,?,?,?,?,?)",
            (run_id, taxon_id, "reactivated", old_sha, sha256, now),
        )
        con.commit()
        return "reactivated"

    # update if sha differs OR sha missing
    if sha256 is None or old_sha != sha256:
        con.execute(
            """
            UPDATE taxa
            SET sci_name=?, swe_name=?, category=?, type=?, status=?, parent_id=?, is_active=?, sha256=?, updated_at=?, raw_json=?
            WHERE taxon_id=?
            """,
            (sci, swe, category, ttype, status, parent_id, 1 if make_active else old_active, sha256, now, raw_json, taxon_id),
        )
        con.execute(
            "INSERT INTO changes(run_id,taxon_id,change_type,old_sha256,new_sha256,at) VALUES(?,?,?,?,?,?)",
            (run_id, taxon_id, "updated", old_sha, sha256, now),
        )
        con.commit()
        return "updated"

    return "unchanged"

def deactivate_missing_species(con: sqlite3.Connection, run_id: int, active_taxon_ids: set[int]) -> int:
    """
    Markera arter som inte längre finns i dagens species-lista som is_active=0.
    Returnerar hur många som deaktiverades.
    """
    now = _now()
    rows = con.execute(
        "SELECT taxon_id, sha256 FROM taxa WHERE is_active=1 AND category='Species'"
    ).fetchall()

    to_deactivate = [int(r["taxon_id"]) for r in rows if int(r["taxon_id"]) not in active_taxon_ids]
    if not to_deactivate:
        return 0

    con.execute("BEGIN IMMEDIATE")
    for tid in to_deactivate:
        old_sha = con.execute("SELECT sha256 FROM taxa WHERE taxon_id=?", (tid,)).fetchone()["sha256"]
        con.execute("UPDATE taxa SET is_active=0, updated_at=? WHERE taxon_id=?", (now, tid))
        con.execute(
            "INSERT INTO changes(run_id,taxon_id,change_type,old_sha256,new_sha256,at) VALUES(?,?,?,?,?,?)",
            (run_id, tid, "deactivated", old_sha, old_sha, now),
        )
    con.commit()
    return len(to_deactivate)
