# Collection Postman — TRPPU ys04

`trppu_collection.json` : collection Postman v2.1 **générée depuis le schéma OpenAPI**
de l'application (73 requêtes, 16 dossiers). Couvre tous les endpoints : scénarios,
TMH, comptages, variations, neutralisations, rétention PIC, trafics, calcul de jours,
sites, produits, versions/coefficients PIC, santé, logs, debug MySQL.

## Import
1. Postman → **Import** → glisser `postman/trppu_collection.json`.
2. La collection embarque ses **variables** (onglet *Variables* de la collection) :

| Variable | Défaut | Rôle |
| -------- | ------ | ---- |
| `baseUrl` | `http://localhost:8080` | URL de base de l'API |
| `id_scenario` | `1` | scénario courant (auto-rempli à la création) |
| `co_regate` | `012345` | code régate du site |
| `co_roc` | `012345` | code roc du site |
| `co_produit` | `OO` | code produit |
| `id_rh` | `A123456` | id RH (crypté en base) |
| `id_pic_version` | `1` | version PIC |
| `id_pic_coef` | `1` | coefficient PIC |
| `id_session_ihm` | `sess-001` | id de session IHM (traçabilité) |

> Adapter `baseUrl` selon le lancement (l'app démarre sur le port **8080** ;
> `uvicorn ... --port 8000` pour le port 8000).

## Parcours de test conseillé
1. **Calcul nombre de jours** → `GET /calcl_nbr_jours/get_nb_jours` (vérifie le socle fériés).
2. **Scenarios** → `POST /trppu-api/scenarios` : crée un scénario ; un script de test
   **capture automatiquement `id_scenario`** dans les variables de collection.
3. **Scenarios** → `GET /{id_scenario}`, `GET /{id_scenario}/periodes`.
4. **TMH / Comptages / Variations / Neutralisations** : `POST`/`PUT` puis `GET` de chaque ressource.
5. **Rétention PIC** → `PUT` puis `GET /{id_scenario}/pic-coefficients`.
6. **Édition** → `GET /trppu-api/scenarios/{id_scenario}/edition` (agrégateur).
7. **MAJ** → `PUT /trppu-api/scenarios/{id_scenario}` (scénario EN COURS).

## Pré-requis serveur
- Variable d'environnement **`ID_RH_CRYPTO_KEY`** (cryptage id_rh).
- Migrations `db_migrations/001→004` appliquées (table fériés + colonnes + enum SAISON).

## Régénérer la collection
```bash
python scripts/gen_postman_collection.py
```
Le script lit `app.main.app.openapi()` : toute nouvelle route est automatiquement incluse.
