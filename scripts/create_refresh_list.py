#!/usr/bin/env python3
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

from dyntaxa_sqlite import db_open, begin_run, end_run, upsert_taxon, deactivate_missing_species

# ========= Config =========
SUBSCRIPTION_KEY = os.getenv("ARTDB_KEY")
if not SUBSCRIPTION_KEY:
    print("Saknar ARTDB_KEY i environment. Kör: export ARTDB_KEY='...'", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,
    "Accept": "application/json",
}

NAMES_URL = "https://api.artdatabanken.se/taxonservice/v1/taxa/names"
CHILDIDS_URL_TEMPLATE = "https://api.artdatabanken.se/taxonservice/v1/taxa/{taxon_id}/childids"

# IMPORTANT: use POST /taxa for details (batch)
TAXA_POST_URL = "https://api.artdatabanken.se/taxonservice/v1/taxa"

TMP_DIR = Path("./tmp")
CACHE_DIR = TMP_DIR / "taxa_cache"

CHILDREN_FILE = TMP_DIR / "children_to_Lepidoptera.json"
SPECIES_IDS_FILE = TMP_DIR / "species_ids_lepidoptera.json"
SPECIES_TABLE_FILE = TMP_DIR / "species_table_lepidoptera.json"

# Refresh-policy:
# 0 = hämta endast taxa som saknas i cache (snabbast, "new only")
# >0 = om cache är äldre än N sekunder, hämta om via POST /taxa
REFRESH_TTL_SECONDS = int(os.getenv("DYNTAXA_CACHE_TTL_SECONDS", "0"))

HTTP_TIMEOUT = 30
POST_BATCH_SIZE = int(os.getenv("DYNTAXA_POST_BATCH_SIZE", "200"))


# ========= Helpers =========
def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _dump_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def _now() -> int:
    return int(time.time())

def _http_get_json(url: str, *, params: dict | None = None, timeout: int = HTTP_TIMEOUT) -> tuple[int, dict | None, dict]:
    """
    Returnerar (status_code, json_obj_or_None, response_headers)
    Vid 404 returnerar json=None.
    """
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

def _http_post_json(url: str, *, params: dict | None = None, body: dict | None = None, timeout: int = HTTP_TIMEOUT) -> tuple[int, Any, dict]:
    """
    Returnerar (status_code, json_payload, response_headers)
    """
    headers = dict(HEADERS)
    # Matcha "Try me"
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


# ========= Dyntaxa-specific =========
def find_taxon_id_lepidoptera() -> int:
    params = {
        "searchString": "Lepidoptera",
        "searchFields": "Both",
        "isRecommended": "NotSet",
        "isOkForObservationSystems": "NotSet",
        "culture": "sv_SE",
        "page": 1,
        "pageSize": 100,
    }
    status, payload, _hdrs = _http_get_json(NAMES_URL, params=params, timeout=HTTP_TIMEOUT)
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

    brief = []
    for it in items:
        ti = it.get("taxonInformation", {}) or {}
        brief.append({
            "taxonId": ti.get("taxonId"),
            "recommendedScientificName": ti.get("recommendedScientificName"),
            "name": it.get("name"),
            "category": (it.get("category", {}) or {}).get("value"),
            "type": (it.get("type", {}) or {}).get("value"),
            "status": (it.get("status", {}) or {}).get("value"),
        })
    raise RuntimeError("Kunde inte entydigt hitta Lepidoptera.\nTräffar:\n" + json.dumps(brief, ensure_ascii=False, indent=2))

def fetch_children_ids(taxon_id: int, out_path: Path) -> dict:
    url = CHILDIDS_URL_TEMPLATE.format(taxon_id=taxon_id)
    params = {"useMainChildren": "false"}
    status, payload, _hdrs = _http_get_json(url, params=params, timeout=60)
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
def _cache_paths(taxon_id: int) -> tuple[Path, Path]:
    # Sprid ut i subdir för att undvika för många filer i samma katalog
    sub = f"{taxon_id // 10000:04d}"
    data_path = CACHE_DIR / sub / f"{taxon_id}.json"
    meta_path = CACHE_DIR / sub / f"{taxon_id}.meta.json"
    return data_path, meta_path

def _cache_needs_refresh(meta: dict) -> bool:
    fetched_at = int(meta.get("fetched_at", 0))
    if fetched_at <= 0:
        return True
    if REFRESH_TTL_SECONDS <= 0:
        return False
    return (_now() - fetched_at) >= REFRESH_TTL_SECONDS

def get_taxon_cached(taxon_id: int) -> dict | None:
    """
    Returnerar taxon-objekt från cache om färskt.
    Returnerar None om saknas (ej hämtad än / eller markerad missing).
    """
    data_path, meta_path = _cache_paths(taxon_id)
    if data_path.exists() and meta_path.exists():
        meta = _read_json(meta_path)
        if not _cache_needs_refresh(meta) and int(meta.get("status", 0)) == 200:
            return _read_json(data_path)
    return None

def _write_cache(taxon_id: int, status: int, payload: dict | None) -> None:
    data_path, meta_path = _cache_paths(taxon_id)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    meta = {
        "taxon_id": taxon_id,
        "status": status,
        "fetched_at": _now(),
    }

    if status == 200 and payload is not None:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        meta["sha256"] = _sha256_bytes(raw)
        _dump_json(data_path, payload)
    else:
        if data_path.exists():
            data_path.unlink(missing_ok=True)

    _dump_json(meta_path, meta)

def _taxon_ids_to_fetch(all_ids: list[int]) -> list[int]:
    """
    Returnerar taxonIds som saknas i cache eller är stale enligt TTL.
    """
    out: list[int] = []
    for tid in all_ids:
        data_path, meta_path = _cache_paths(tid)
        if not meta_path.exists() or not data_path.exists():
            out.append(tid)
            continue
        try:
            meta = _read_json(meta_path)
        except Exception:
            out.append(tid)
            continue
        if _cache_needs_refresh(meta):
            out.append(tid)
    return out

def _chunk(seq: list[int], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def refresh_taxa_cache_batch(taxon_ids: list[int], *, culture: str = "sv_SE") -> int:
    """
    Hämtar (via POST) taxonobjekt för de ids som behöver.
    Returnerar antal objekt skrivna som 200.
    """
    to_fetch = _taxon_ids_to_fetch(taxon_ids)
    if not to_fetch:
        return 0

    written_ok = 0
    params = {"culture": culture}

    for batch in _chunk(to_fetch, POST_BATCH_SIZE):
        status, payload, _hdrs = _http_post_json(
            TAXA_POST_URL,
            params=params,
            body={"taxonIds": batch},
            timeout=max(HTTP_TIMEOUT, 60),
        )

        if status != 200 or not isinstance(payload, list):
            raise RuntimeError(f"Oväntat svar från POST /taxa: status={status} payload_type={type(payload)}")

        returned_ids = set()
        for obj in payload:
            if not isinstance(obj, dict) or "taxonId" not in obj:
                continue
            tid = int(obj["taxonId"])
            returned_ids.add(tid)
            _write_cache(tid, 200, obj)
            written_ok += 1

        # Om API:t inte returnerar vissa ids: markera som missing så vi inte loopar hårt på dem.
        # (Det kan vara pseudotaxa/icke-taxonomiskt eller annat som inte returneras här.)
        for tid in batch:
            if tid not in returned_ids:
                _write_cache(tid, 404, None)

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


# ========= Main pipeline =========
def main() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    lepidoptera_id = find_taxon_id_lepidoptera()
    print(f"Dyntaxa database is online, Lepidoptera found as TaxonId {lepidoptera_id}, continuing ...")

    child_payload = fetch_children_ids(lepidoptera_id, CHILDREN_FILE)
    print(f"Saved child ids to: {CHILDREN_FILE}")

    child_ids = _extract_child_ids(child_payload)
    print(f"Child ids count: {len(child_ids)}")

    # Refresh cache in batches (only new or TTL-expired)
    before_missing = sum(1 for tid in child_ids if not _cache_paths(tid)[1].exists())
    written_ok = refresh_taxa_cache_batch(child_ids, culture="sv_SE")

    species_ids: list[int] = []
    species_table: list[dict] = []

    skipped_missing = 0
    for tid in child_ids:
        obj = get_taxon_cached(tid)
        if obj is None:
            skipped_missing += 1
            continue
        if is_species_accepted_taxonomic(obj):
            species_ids.append(tid)
            species_table.append(extract_names(obj))

    _dump_json(SPECIES_IDS_FILE, {"lepidopteraTaxonId": lepidoptera_id, "speciesTaxonIds": species_ids})
    _dump_json(SPECIES_TABLE_FILE, {"lepidopteraTaxonId": lepidoptera_id, "species": species_table})

    print(f"Species count (Accepted/Taxonomic): {len(species_ids)}")
    print(f"Cache miss before run: {before_missing}")
    print(f"Fetched/updated this run (200 OK): {written_ok}")
    print(f"Skipped non-returned taxa (cached as 404/missing): {skipped_missing}")
    print(f"Wrote: {SPECIES_IDS_FILE}")
    print(f"Wrote: {SPECIES_TABLE_FILE}")



    DB_PATH = Path("./tmp/dyntaxa_lepidoptera.sqlite")
    con = db_open(DB_PATH)

    run_id = begin_run(con, lepidoptera_id, len(child_ids))
    
    inserted = updated = 0
    active_species: set[int] = set()

    for tid in child_ids:
        obj = get_taxon_cached(tid)
        if obj is None:
            continue

        if is_species_accepted_taxonomic(obj):
            # hämta sha256 från din cache-meta om du vill
            _data_path, meta_path = _cache_paths(tid)
            sha = None
            if meta_path.exists():
                meta = _read_json(meta_path)
                sha = meta.get("sha256")

            change = upsert_taxon(con, run_id, obj, sha, make_active=True)
            if change == "inserted":
                inserted += 1
            elif change in ("updated", "reactivated"):
                updated += 1

            active_species.add(tid)

    deactivated = deactivate_missing_species(con, run_id, active_species)

    end_run(con, run_id,
            species_count=len(active_species),
            inserted=inserted,
            updated=updated,
            deactivated=deactivated)

    print(f"SQLite: inserted={inserted}, updated/reactivated={updated}, deactivated={deactivated}")
    print(f"DB: {DB_PATH}")


if __name__ == "__main__":
    main()
