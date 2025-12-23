#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

import requests

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

def _get_json(url: str, *, params: dict | None = None, timeout: int = 30) -> dict:
    r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
    if not r.ok:
        # Bra felutskrift när API:t svarar med JSON-fel
        try:
            body = r.json()
            pretty = json.dumps(body, ensure_ascii=False, indent=2)
        except Exception:
            pretty = r.text
        print(f"HTTP {r.status_code} {r.reason}\nURL: {r.url}\nBody:\n{pretty}", file=sys.stderr)
        r.raise_for_status()
    return r.json()

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
    payload = _get_json(NAMES_URL, params=params, timeout=30)

    items = payload.get("data", [])
    if not items:
        raise RuntimeError(f"Inga träffar i 'data'. Svar: {payload}")

    # Robust matchning: exakt vetenskapligt namn + taxonomiskt + rätt kategori (Order)
    for it in items:
        ti = it.get("taxonInformation", {}) or {}
        rec_sci = ti.get("recommendedScientificName")
        category = (it.get("category", {}) or {}).get("value")
        ttype = (it.get("type", {}) or {}).get("value")
        status = (it.get("status", {}) or {}).get("value")

        if (
            rec_sci == "Lepidoptera"
            and category == "Order"
            and ttype == "Taxonomic"
            and status == "Accepted"
        ):
            return int(ti["taxonId"])

    # Fallback: om fälten skulle skilja sig mellan kulturer/uppdateringar,
    # välj posten med name=="Lepidoptera" och taxonInformation.recommendedScientificName=="Lepidoptera"
    for it in items:
        ti = it.get("taxonInformation", {}) or {}
        if it.get("name") == "Lepidoptera" and ti.get("recommendedScientificName") == "Lepidoptera":
            return int(ti["taxonId"])

    # Om inget matchar, dumpa en komprimerad lista så du ser vad som kom tillbaka
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

def fetch_children_ids(taxon_id: int, out_path: Path) -> None:
    url = CHILDIDS_URL_TEMPLATE.format(taxon_id=taxon_id)
    params = {"useMainChildren": "false"}  # exakt som din Try me
    payload = _get_json(url, params=params, timeout=60)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def main() -> None:
    lepidoptera_id = find_taxon_id_lepidoptera()
    print(f"Dyntaxa database is online, Lepidoptera found as TaxonId {lepidoptera_id}, continuing ...")

    out_file = Path("./tmp/children_to_Lepidoptera.json")
    fetch_children_ids(lepidoptera_id, out_file)
    print(f"Saved child ids to: {out_file}")

if __name__ == "__main__":
    main()
