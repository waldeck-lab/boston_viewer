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

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

# Logger
import logging
from logging.handlers import RotatingFileHandler

import requests

from dyntaxa_sqlite import db_open, begin_run, end_run, upsert_taxon, deactivate_missing_species


# Repo root
REPO_ROOT = Path(__file__).resolve().parents[1]

DATA_ROOT_DEFAULT = Path(os.getenv("DYNTAXA_DATA_ROOT", str(REPO_ROOT / "data")))
CACHE_ROOT_DEFAULT = Path(os.getenv("DYNTAXA_CACHE_ROOT", str(DATA_ROOT_DEFAULT / "cache")))
DB_ROOT_DEFAULT = Path(os.getenv("DYNTAXA_DB_ROOT", str(DATA_ROOT_DEFAULT / "db")))

DB_PATH_DEFAULT = Path(os.getenv("DYNTAXA_DB", str(DB_ROOT_DEFAULT / "dyntaxa_lepidoptera.sqlite")))


# ========= Config (API key stays in env) =========
SUBSCRIPTION_KEY = os.getenv("ARTDATABANKEN_SUBSCRIPTION_KEY")
if not SUBSCRIPTION_KEY:
    print("Saknar ARTDB_KEY i environment. Kör: export ARTDATABANKEN_SUBSCRIPTION_KEY='...'", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,
    "Accept": "application/json",
}

NAMES_URL = "https://api.artdatabanken.se/taxonservice/v1/taxa/names"
CHILDIDS_URL_TEMPLATE = "https://api.artdatabanken.se/taxonservice/v1/taxa/{taxon_id}/childids"
TAXA_POST_URL = "https://api.artdatabanken.se/taxonservice/v1/taxa"

HTTP_TIMEOUT_DEFAULT = 30

REFRESH_TTL_SECONDS_DEFAULT = int(os.getenv("DYNTAXA_CACHE_TTL_SECONDS", "0"))
POST_BATCH_SIZE_DEFAULT = int(os.getenv("DYNTAXA_POST_BATCH_SIZE", "200"))
FAST_EXIT_ON_UNCHANGED_SOURCE_DEFAULT = os.getenv("DYNTAXA_FAST_EXIT", "1") == "1"

DEFAULT_VERBOSE = os.getenv("DYNTAXA_VERBOSE", "1") == "1"


# ========= Logging =========
REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = Path(os.getenv("DYNTAXA_LOG_DIR", str(REPO_ROOT / "logs")))
LOG_FILE = LOG_DIR / "dyntaxa_refresh.log"

def setup_logging(verbose: bool = False) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("dyntaxa")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler (append, roterande)
    fh = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # Console handler (INFO eller DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    # Undvik dubbla handlers vid import/test
    logger.propagate = False

    return logger

# ========= Helpers =========
def _now() -> int:
    return int(time.time())

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _dump_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def taxon_sha256(obj: dict) -> str:
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return _sha256_bytes(raw)

def _stable_ids_hash(lepidoptera_id: int, child_ids: list[int]) -> str:
    ids = sorted(int(x) for x in child_ids)
    raw = json.dumps(
        {"root": int(lepidoptera_id), "childIds": ids},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return _sha256_bytes(raw)

def _http_get_json(url: str, *, params: dict | None = None, timeout: int) -> tuple[int, dict | None, dict]:
    r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)

    if r.status_code == 404:
        return 404, None, dict(r.headers)

    if not r.ok:
        try:
            body = r.json()
            pretty = json.dumps(body, ensure_ascii=False, indent=2)
        except Exception:
            pretty = r.text
        print(f"HTTP {r.status_code} {r.reason}\nURL: {r.url}\nBody:\n{pretty}", file=sys.stderr)
        r.raise_for_status()

    return r.status_code, r.json(), dict(r.headers)

def _http_post_json(url: str, *, params: dict | None = None, body: dict | None = None, timeout: int) -> tuple[int, Any, dict]:
    headers = dict(HEADERS)
    headers["Content-Type"] = "application/json-patch+json"

    r = requests.post(url, headers=headers, params=params, json=body, timeout=timeout)

    if not r.ok:
        try:
            j = r.json()
            pretty = json.dumps(j, ensure_ascii=False, indent=2)
        except Exception:
            pretty = r.text
        print(f"HTTP {r.status_code} {r.reason}\nURL: {r.url}\nBody:\n{pretty}", file=sys.stderr)
        r.raise_for_status()

    return r.status_code, r.json(), dict(r.headers)

def _chunk(seq: list[int], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ========= Dyntaxa-specific =========
def find_taxon_id_lepidoptera(*, culture: str, timeout: int) -> int:
    params = {
        "searchString": "Lepidoptera",
        "searchFields": "Both",
        "isRecommended": "NotSet",
        "isOkForObservationSystems": "NotSet",
        "culture": culture,
        "page": 1,
        "pageSize": 100,
    }
    status, payload, _hdrs = _http_get_json(NAMES_URL, params=params, timeout=timeout)
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"Oväntat svar från names: status={status} payload={payload}")

    items = payload.get("data", [])
    if not items:
        raise RuntimeError(f"Inga träffar i 'data'. Svar: {payload}")

    for it in items:
        ti = it.get("taxonInformation", {}) or {}
        rec_sci = ti.get("recommendedScientificName")
        category = (it.get("category", {}) or {}).get("value")
        ttype = (it.get("type", {}) or {}).get("value")
        statusv = (it.get("status", {}) or {}).get("value")
        if rec_sci == "Lepidoptera" and category == "Order" and ttype == "Taxonomic" and statusv == "Accepted":
            return int(ti["taxonId"])

    for it in items:
        ti = it.get("taxonInformation", {}) or {}
        if it.get("name") == "Lepidoptera" and ti.get("recommendedScientificName") == "Lepidoptera":
            return int(ti["taxonId"])

    raise RuntimeError("Kunde inte entydigt hitta Lepidoptera.")

def fetch_children_ids(taxon_id: int, *, out_path: Path, timeout: int) -> dict:
    url = CHILDIDS_URL_TEMPLATE.format(taxon_id=taxon_id)
    params = {"useMainChildren": "false"}
    status, payload, _hdrs = _http_get_json(url, params=params, timeout=timeout)
    if status != 200 or payload is None:
        raise RuntimeError(f"Misslyckades hämta childids: status={status} payload={payload}")
    _dump_json(out_path, payload)
    return payload

def _extract_child_ids(child_ids_payload: Any) -> list[int]:
    if isinstance(child_ids_payload, list):
        ids = child_ids_payload
    elif isinstance(child_ids_payload, dict):
        ids = child_ids_payload.get("taxonIds") or child_ids_payload.get("data") or []
    else:
        ids = []
    return [int(x) for x in ids]


# ========= Cache =========
def _cache_paths(cache_dir: Path, taxon_id: int) -> tuple[Path, Path]:
    sub = f"{taxon_id // 10000:04d}"
    data_path = cache_dir / sub / f"{taxon_id}.json"
    meta_path = cache_dir / sub / f"{taxon_id}.meta.json"
    return data_path, meta_path

def _cache_needs_refresh(meta: dict, ttl_seconds: int) -> bool:
    fetched_at = int(meta.get("fetched_at", 0))
    if fetched_at <= 0:
        return True
    if ttl_seconds <= 0:
        return False
    return (_now() - fetched_at) >= ttl_seconds

def get_taxon_cached(cache_dir: Path, taxon_id: int, ttl_seconds: int) -> dict | None:
    data_path, meta_path = _cache_paths(cache_dir, taxon_id)
    if data_path.exists() and meta_path.exists():
        meta = _read_json(meta_path)
        if not _cache_needs_refresh(meta, ttl_seconds) and int(meta.get("status", 0)) == 200:
            return _read_json(data_path)
    return None

def _write_cache(cache_dir: Path, taxon_id: int, status: int, payload: dict | None) -> None:
    data_path, meta_path = _cache_paths(cache_dir, taxon_id)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    meta = {
        "taxon_id": taxon_id,
        "status": status,
        "fetched_at": _now(),
    }

    if status == 200 and payload is not None:
        meta["sha256"] = taxon_sha256(payload)
        _dump_json(data_path, payload)
    else:
        if data_path.exists():
            data_path.unlink(missing_ok=True)

    _dump_json(meta_path, meta)

def _taxon_ids_to_fetch(cache_dir: Path, all_ids: list[int], ttl_seconds: int) -> list[int]:
    out: list[int] = []
    for tid in all_ids:
        data_path, meta_path = _cache_paths(cache_dir, tid)
        if not meta_path.exists() or not data_path.exists():
            out.append(tid)
            continue
        try:
            meta = _read_json(meta_path)
        except Exception:
            out.append(tid)
            continue
        if _cache_needs_refresh(meta, ttl_seconds):
            out.append(tid)
    return out

def refresh_taxa_cache_batch(
    cache_dir: Path,
    taxon_ids: list[int],
    *,
    culture: str,
    ttl_seconds: int,
    batch_size: int,
    timeout: int,
) -> int:
    to_fetch = _taxon_ids_to_fetch(cache_dir, taxon_ids, ttl_seconds)
    if not to_fetch:
        return 0

    written_ok = 0
    params = {"culture": culture}

    for batch in _chunk(to_fetch, batch_size):
        status, payload, _hdrs = _http_post_json(
            TAXA_POST_URL,
            params=params,
            body={"taxonIds": batch},
            timeout=max(timeout, 60),
        )

        if status != 200 or not isinstance(payload, list):
            raise RuntimeError(f"Oväntat svar från POST /taxa: status={status} payload_type={type(payload)}")

        returned_ids = set()
        for obj in payload:
            if not isinstance(obj, dict) or "taxonId" not in obj:
                continue
            tid = int(obj["taxonId"])
            returned_ids.add(tid)
            _write_cache(cache_dir, tid, 200, obj)
            written_ok += 1

        for tid in batch:
            if tid not in returned_ids:
                _write_cache(cache_dir, tid, 404, None)

    return written_ok


# ========= Filtering / extraction =========
def is_species_accepted_taxonomic(taxon_obj: dict) -> bool:
    cat = (taxon_obj.get("category") or {}).get("value")
    ttype = (taxon_obj.get("type") or {}).get("value")
    statusv = (taxon_obj.get("status") or {}).get("value")
    return (cat == "Species") and (ttype == "Taxonomic") and (statusv == "Accepted")

def _recommended_name(taxon_obj: dict, name_category_value: str) -> str | None:
    for n in taxon_obj.get("names", []) or []:
        cat = (n.get("category") or {}).get("value")
        if cat == name_category_value and n.get("isRecommended") is True:
            return n.get("name")
    return None

def extract_names(taxon_obj: dict) -> dict:
    sci = _recommended_name(taxon_obj, "ScientificName")
    swe = _recommended_name(taxon_obj, "SwedishName")
    genus = sci.split(" ", 1)[0] if isinstance(sci, str) and " " in sci else (sci if isinstance(sci, str) else None)

    return {
        "taxonId": int(taxon_obj.get("taxonId")),
        "scientificName": sci,
        "swedishName": swe,
        "genus": genus,
        "category": (taxon_obj.get("category") or {}).get("value"),
        "type": (taxon_obj.get("type") or {}).get("value"),
        "status": (taxon_obj.get("status") or {}).get("value"),
    }


# ========= Source revision file =========
def load_source_rev(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return _read_json(path)
    except Exception:
        return None

def write_source_rev(path: Path, lepidoptera_id: int, child_ids: list[int], source_hash: str) -> None:
    _dump_json(path, {
        "lepidopteraTaxonId": int(lepidoptera_id),
        "childCount": len(child_ids),
        "sourceHash": source_hash,
        "updatedAt": _now(),
    })


# ========= CLI =========
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Refresh local Lepidoptera species cache and SQLite database (Dyntaxa).")

    p.add_argument("--force", action="store_true", help="Run even if source revision unchanged (ignore fast-exit).")
    p.add_argument("--no-sqlite", action="store_true", help="Skip SQLite update step.")
    p.add_argument("--only-refresh-cache", action="store_true", help="Only refresh cache (POST /taxa batches).")
    p.add_argument("--only-build-lists", action="store_true", help="Only build lists from cache; do not refresh via POST /taxa.")

    p.add_argument("--culture", default=os.getenv("DYNTAXA_CULTURE", "sv_SE"), help="Culture param (default: sv_SE).")
    p.add_argument("--ttl-seconds", type=int, default=REFRESH_TTL_SECONDS_DEFAULT, help="Cache TTL seconds (0 = new only).")
    p.add_argument("--batch-size", type=int, default=POST_BATCH_SIZE_DEFAULT, help="POST /taxa batch size.")
    p.add_argument("--timeout", type=int, default=HTTP_TIMEOUT_DEFAULT, help="HTTP timeout seconds.")

    p.add_argument("--tmp-dir",type=Path,default=Path(os.getenv("DYNTAXA_TMP_DIR", str(CACHE_ROOT_DEFAULT))),help="Cache root dir (children/species lists + taxa_cache).",)

    p.add_argument("--db",type=Path,default=Path(os.getenv("DYNTAXA_DB", str(DB_ROOT_DEFAULT / "dyntaxa_lepidoptera.sqlite"))),help="SQLite db path.",)
    
    p.add_argument("--fast-exit", action="store_true", default=FAST_EXIT_ON_UNCHANGED_SOURCE_DEFAULT, help="Fast exit when source revision unchanged.")
    p.add_argument("--no-fast-exit", dest="fast_exit", action="store_false", help="Disable fast exit when source revision unchanged.")
    p.add_argument(
        "--verbose",
        action="store_true",
        default=DEFAULT_VERBOSE,
        help="Verbose logging (default via DYNTAXA_VERBOSE).",
    )
    p.add_argument(
        "--quiet",
        dest="verbose",
        action="store_false",
        help="Disable verbose logging.",
    )

    args = p.parse_args()

    if args.only_refresh_cache and args.only_build_lists:
        p.error("Choose only one of --only-refresh-cache and --only-build-lists.")

    return args


# ========= Main pipeline =========
def main() -> None:
    args = parse_args()
    logger = setup_logging(verbose=args.verbose)
    logger.info("=== Dyntaxa refresh started ===")

    tmp_dir: Path = args.tmp_dir
    cache_dir = tmp_dir / "taxa_cache"

    children_file = tmp_dir / "children_to_Lepidoptera.json"
    species_ids_file = tmp_dir / "species_ids_lepidoptera.json"
    species_table_file = tmp_dir / "species_table_lepidoptera.json"
    source_rev_file = tmp_dir / "lepidoptera_source_rev.json"

    tmp_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    lepidoptera_id = find_taxon_id_lepidoptera(culture=args.culture, timeout=args.timeout)
    #print(f"Dyntaxa database is online, Lepidoptera found as TaxonId {lepidoptera_id}, continuing ...")
    logger.info("Dyntaxa database is online")
    
    child_payload = fetch_children_ids(lepidoptera_id, out_path=children_file, timeout=max(args.timeout, 60))
    #print(f"Saved child ids to: {children_file}")

    child_ids = _extract_child_ids(child_payload)
    #print(f"Child ids count: {len(child_ids)}")

    logger.info(
        "Lepidoptera taxonId=%d, childIds=%d",
        lepidoptera_id,
        len(child_ids),
    )
    
    source_hash = _stable_ids_hash(lepidoptera_id, child_ids)
    prev = load_source_rev(source_rev_file)

    source_unchanged = (
        prev
        and prev.get("sourceHash") == source_hash
        and int(prev.get("lepidopteraTaxonId", 0)) == int(lepidoptera_id)
    )

    if source_unchanged:
        #print("Source revision unchanged (root + childIds).")
        logger.info("Source revision unchanged (root + childIds).")
        if args.fast_exit and not args.force:
            #print("Fast-exit enabled => exiting early. Use --force or --no-fast-exit to override.")
            logger.info("Source unchanged → fast-exit")
            logger.info("=== Dyntaxa refresh finished ===")
            return

    # Refresh cache unless explicitly disabled
    before_missing = sum(1 for tid in child_ids if not _cache_paths(cache_dir, tid)[1].exists())
    written_ok = 0

    if not args.only_build_lists:
        written_ok = refresh_taxa_cache_batch(
            cache_dir,
            child_ids,
            culture=args.culture,
            ttl_seconds=args.ttl_seconds,
            batch_size=args.batch_size,
            timeout=args.timeout,
        )

    if args.only_refresh_cache:
        write_source_rev(source_rev_file, lepidoptera_id, child_ids, source_hash)
        #print(f"Cache miss before run: {before_missing}")
        #print(f"Fetched/updated this run (200 OK): {written_ok}")
        #print(f"Source rev: {source_rev_file}")
        logger.info(
            "Cache: miss_before=%d fetched_ok=%d source_rev_file =%d",
            before_missing,
            written_ok,
            source_rev_file,
        )
        logger.info("=== Dyntaxa refresh finished ===")
        return

    # Build lists from cache
    species_ids: list[int] = []
    species_table: list[dict] = []

    skipped_missing = 0
    for tid in child_ids:
        obj = get_taxon_cached(cache_dir, tid, args.ttl_seconds)
        if obj is None:
            skipped_missing += 1
            continue
        if is_species_accepted_taxonomic(obj):
            species_ids.append(tid)
            species_table.append(extract_names(obj))

    _dump_json(species_ids_file, {"lepidopteraTaxonId": lepidoptera_id, "speciesTaxonIds": species_ids})
    _dump_json(species_table_file, {"lepidopteraTaxonId": lepidoptera_id, "species": species_table})

    #print(f"Species count (Accepted/Taxonomic): {len(species_ids)}")
    #print(f"Cache miss before run: {before_missing}")
    #print(f"Fetched/updated this run (200 OK): {written_ok}")
    #print(f"Skipped non-returned taxa (cached as 404/missing): {skipped_missing}")
    #print(f"Wrote: {species_ids_file}")
    #print(f"Wrote: {species_table_file}")

    logger.info(
        "Species count (Accepted/Taconomic)=%d, Cache: miss_before=%d fetched_ok=%d skipped_missing=%d",
        len(species_ids),
        before_missing,
        written_ok,
        skipped_missing,
    )
    logger.info("Wrote: %s, %s",species_ids_file, species_table_file)

    # SQLite step
    if args.no_sqlite:
        write_source_rev(source_rev_file, lepidoptera_id, child_ids, source_hash)
        #print("SQLite: skipped (--no-sqlite)")
        #print(f"Source rev: {source_rev_file}")
        logger.info("SQLite: skipped (--no-sqlite)")
        logger.info("=== Dyntaxa refresh finished ===")
        return

    con = db_open(args.db)
    run_id = begin_run(con, lepidoptera_id, len(child_ids), source_hash=source_hash)

    inserted = updated = unchanged = 0
    active_species: set[int] = set()

    for tid in child_ids:
        obj = get_taxon_cached(cache_dir, tid, args.ttl_seconds)
        if obj is None:
            continue
        if not is_species_accepted_taxonomic(obj):
            continue

        _data_path, meta_path = _cache_paths(cache_dir, tid)
        sha = None
        if meta_path.exists():
            try:
                meta = _read_json(meta_path)
                sha = meta.get("sha256")
            except Exception:
                sha = None
        if sha is None:
            sha = taxon_sha256(obj)

        change = upsert_taxon(con, run_id, obj, sha, make_active=True)
        if change == "inserted":
            inserted += 1
        elif change in ("updated", "reactivated"):
            updated += 1
        else:
            unchanged += 1

        active_species.add(tid)

    deactivated = deactivate_missing_species(con, run_id, active_species)

    end_run(
        con,
        run_id,
        species_count=len(active_species),
        inserted=inserted,
        updated=updated,
        unchanged=unchanged,
        deactivated=deactivated,
    )

    write_source_rev(source_rev_file, lepidoptera_id, child_ids, source_hash)

    #print(f"SQLite: inserted={inserted}, updated/reactivated={updated}, unchanged={unchanged}, deactivated={deactivated}")
    #print(f"DB: {args.db}")
    #print(f"Source rev: {source_rev_file}")

    logger.info(
        "SQLite: inserted=%d updated=%d unchanged=%d deactivated=%d",
        inserted,
        updated,
        unchanged,
        deactivated,
    )
    logger.info("=== Dyntaxa refresh finished ===")



if __name__ == "__main__":
    main()
