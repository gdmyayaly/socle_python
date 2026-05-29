"""Génère une collection Postman v2.1 à partir du schéma OpenAPI de l'app.

Couvre TOUS les endpoints exposés (existants + tickets DSR). Pré-remplit des
exemples de corps de requête et des variables de collection.

Usage :
    python scripts/gen_postman_collection.py
Sortie : postman/trppu_collection.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.main import app  # noqa: E402

OUTPUT = ROOT / "postman" / "trppu_collection.json"

# Valeurs d'exemple par nom de propriété (corps & query).
OVERRIDES = {
    "co_regate": "{{co_regate}}",
    "co_roc": "{{co_roc}}",
    "id_rh": "{{id_rh}}",
    "co_produit": "{{co_produit}}",
    "id_session_ihm": "{{id_session_ihm}}",
    "lb_scenario": "Scénario test",
    "lb_regate": "PARIS PDC",
    "type_site": "PDC",
    "nb_jours_semaine": 6,
    "periode_debut": "2026-01-01",
    "periode_fin": "2026-12-31",
    "dt_mise_en_oeuvre": "2026-06-01",
    "dt_comptage": "2026-05-29",
    "nb_produit": 1500,
    "variation_pct": 25.00,
    "moyenne_journaliere": 4000.00,
    "moyenne_hebdo": 24000.00,
    "volume_realise": 120000,
    "volume_previsionnel": 130000,
    "exclusion": False,
    "coef": 0.85,
    "densite": 0,
    "jour_semaine": "LUNDI",
    "type": "PEAK",
    "dt_debut": "2026-11-10",
    "dt_fin": "2026-12-19",
    "id_pic_version": 1,
    "niveau": "SCENARIO",
    "statut": "VALIDE",
    "est_fige": False,
}

# Variables de collection (modifiables dans Postman).
VARIABLES = {
    "baseUrl": "http://localhost:8080",
    "id_scenario": "1",
    "co_regate": "012345",
    "co_roc": "012345",
    "co_produit": "OO",
    "id_rh": "A123456",
    "id_pic_version": "1",
    "id_pic_coef": "1",
    "id_session_ihm": "sess-001",
}

BODY_METHODS = {"post", "put", "patch"}

# Scripts de test Postman : capture de variables depuis la réponse.
CAPTURES = {
    ("POST", "/trppu-api/scenarios"): [
        "var j = pm.response.json();",
        "if (j && j.id_scenario) {",
        "  pm.collectionVariables.set('id_scenario', j.id_scenario);",
        "  console.log('id_scenario =', j.id_scenario);",
        "}",
    ],
}


def _resolve(schema: dict, components: dict) -> dict:
    if "$ref" in schema:
        name = schema["$ref"].split("/")[-1]
        return components.get(name, {})
    return schema


def _example(schema: dict, components: dict, prop_name: str | None = None):
    if prop_name and prop_name in OVERRIDES:
        return OVERRIDES[prop_name]

    schema = _resolve(schema, components)

    if "example" in schema:
        return schema["example"]
    if "default" in schema and schema["default"] is not None:
        return schema["default"]
    if "enum" in schema and schema["enum"]:
        return schema["enum"][0]

    for key in ("anyOf", "oneOf"):
        if key in schema:
            for sub in schema[key]:
                rs = _resolve(sub, components)
                if rs.get("type") != "null":
                    return _example(sub, components, prop_name)
    if "allOf" in schema and schema["allOf"]:
        return _example(schema["allOf"][0], components, prop_name)

    t = schema.get("type")
    if t == "object" or "properties" in schema:
        out = {}
        for name, sub in schema.get("properties", {}).items():
            out[name] = _example(sub, components, name)
        return out
    if t == "array":
        items = schema.get("items", {})
        return [_example(items, components, prop_name)]
    if t == "string":
        fmt = schema.get("format")
        if fmt == "date":
            return "2026-01-01"
        if fmt == "date-time":
            return "2026-01-01T00:00:00"
        return "string"
    if t == "integer":
        return 0
    if t == "number":
        return 0
    if t == "boolean":
        return False
    return None


def _url(path: str, query_items: list) -> dict:
    pm_path = path.replace("{", "{{").replace("}", "}}")
    segments = [s for s in pm_path.split("/") if s != ""]
    raw = "{{baseUrl}}" + pm_path
    if query_items:
        raw += "?" + "&".join(
            f"{q['key']}={q['value']}" for q in query_items if not q.get("disabled")
        )
    url = {"raw": raw, "host": ["{{baseUrl}}"], "path": segments}
    if query_items:
        url["query"] = query_items
    return url


def build() -> dict:
    spec = app.openapi()
    components = spec.get("components", {}).get("schemas", {})
    folders: dict[str, dict] = {}

    for path, methods in spec["paths"].items():
        for method, op in methods.items():
            if method not in ("get", "post", "put", "patch", "delete"):
                continue
            tag = (op.get("tags") or ["Autres"])[0]
            folder = folders.setdefault(tag, {"name": tag, "item": []})

            # Query params
            query_items = []
            for p in op.get("parameters", []):
                if p.get("in") != "query":
                    continue
                name = p["name"]
                val = _example(p.get("schema", {}), components, name)
                if val is None:
                    val = ""
                query_items.append(
                    {
                        "key": name,
                        "value": str(val),
                        "disabled": not p.get("required", False),
                        "description": p.get("description", ""),
                    }
                )

            request = {
                "method": method.upper(),
                "header": [],
                "url": _url(path, query_items),
                "description": op.get("summary", "") or op.get("description", ""),
            }

            # Corps de requête
            if method in BODY_METHODS:
                content = (op.get("requestBody", {}) or {}).get("content", {})
                json_schema = content.get("application/json", {}).get("schema")
                if json_schema:
                    body = _example(json_schema, components)
                    request["header"].append(
                        {"key": "Content-Type", "value": "application/json"}
                    )
                    request["body"] = {
                        "mode": "raw",
                        "raw": json.dumps(body, ensure_ascii=False, indent=2),
                        "options": {"raw": {"language": "json"}},
                    }

            item = {"name": f"{method.upper()} {path}", "request": request}
            capture = CAPTURES.get((method.upper(), path))
            if capture:
                item["event"] = [
                    {
                        "listen": "test",
                        "script": {"type": "text/javascript", "exec": capture},
                    }
                ]
            folder["item"].append(item)

    collection = {
        "info": {
            "name": "TRPPU ys04 — API (DSR)",
            "description": "Collection générée depuis OpenAPI. Couvre tous les endpoints "
            "(scénarios, TMH, comptages, variations, neutralisations, PIC, trafics, jours...).",
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
        },
        "item": [folders[k] for k in folders],
        "variable": [{"key": k, "value": v} for k, v in VARIABLES.items()],
    }
    return collection


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    collection = build()
    OUTPUT.write_text(
        json.dumps(collection, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    nb = sum(len(f["item"]) for f in collection["item"])
    print(f"=> {nb} requêtes dans {len(collection['item'])} dossiers -> {OUTPUT}")


if __name__ == "__main__":
    main()
