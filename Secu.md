# Guide d'audit securite & performance - socle_python

Ce document explique pas a pas comment reproduire l'analyse de securite et de performance du projet, et comment exploiter les resultats.

---

## Prerequis

```bash
pip install bandit pip-audit safety
```

Ces dependances sont deja listees dans `requirements.txt` sous la section "Audit securite".

---

## Etape 1 : Scan des vulnerabilites dans le code (Bandit)

### Qu'est-ce que Bandit ?

Bandit est un outil d'analyse statique qui parcourt le code Python pour detecter les problemes de securite courants (injections SQL, binding reseau, secrets hardcodes, etc.).

### Lancer le scan

```bash
# Scan complet avec rapport JSON lisible
bandit -r app/ -f json 2>/dev/null | python -m json.tool

# Scan complet avec rapport texte (plus lisible dans le terminal)
bandit -r app/

# Scan avec niveau de severite minimum (MEDIUM ou HIGH uniquement)
bandit -r app/ -ll

# Scan d'un fichier specifique
bandit app/routes/trafics.py
```

### Lire les resultats

Chaque alerte contient :
- **test_id** : Identifiant de la regle (ex: B608 = SQL injection, B104 = bind all interfaces)
- **issue_severity** : LOW, MEDIUM ou HIGH
- **issue_confidence** : Niveau de certitude (LOW, MEDIUM, HIGH)
- **issue_cwe** : Reference CWE (Common Weakness Enumeration) avec lien vers la documentation
- **filename / line_number** : Localisation exacte dans le code
- **code** : Extrait du code concerne

### Exploiter les resultats

1. **Trier par severite** : Traiter d'abord les HIGH, puis MEDIUM, puis LOW
2. **Verifier les faux positifs** : Bandit peut signaler du code volontairement ecrit ainsi. Ajouter `# nosec` en commentaire pour les ignorer (avec justification)
3. **Pour chaque alerte** :
   - Ouvrir le fichier et la ligne indiques
   - Lire la documentation CWE liee (le lien est dans le rapport JSON)
   - Appliquer la correction recommandee (voir section "Corrections types" ci-dessous)

### Codes Bandit frequents dans ce projet

| Code | Nom | Description | Correction |
|------|-----|-------------|------------|
| B104 | hardcoded_bind_all_interfaces | Serveur ecoute sur 0.0.0.0 | Limiter a 127.0.0.1 en prod |
| B608 | hardcoded_sql_expressions | SQL construit par f-string/concatenation | Utiliser des requetes parametrees |
| B105 | hardcoded_password_string | Mot de passe en dur | Passer par variable d'environnement |
| B301 | pickle | Deserialisation pickle | Eviter pickle, utiliser JSON |

---

## Etape 2 : Scan des dependances (pip-audit)

### Qu'est-ce que pip-audit ?

pip-audit verifie si les packages Python installes contiennent des vulnerabilites connues (CVE) en consultant la base de donnees PyPI Advisory.

### Lancer le scan

```bash
# Scan depuis le fichier requirements
pip-audit -r requirements.txt

# Scan de l'environnement installe
pip-audit

# Scan avec sortie JSON
pip-audit -f json -r requirements.txt

# Scan strict (echoue si vulnerabilite trouvee - utile en CI)
pip-audit -r requirements.txt --strict
```

### Lire les resultats

Le rapport liste pour chaque vulnerabilite :
- **Name** : Nom du package
- **Version** : Version installee
- **ID** : Identifiant CVE ou PYSEC
- **Fix Versions** : Versions corrigees

### Exploiter les resultats

1. Si une vulnerabilite est trouvee :
   ```bash
   # Mettre a jour le package concerne
   pip install --upgrade <package>
   
   # Relancer le scan pour verifier
   pip-audit -r requirements.txt
   ```
2. Si la mise a jour casse la compatibilite, documenter la decision dans ce fichier
3. En CI/CD, utiliser `--strict` pour bloquer le build si une vulnerabilite est detectee

---

## Etape 3 : Scan complementaire avec Safety

### Qu'est-ce que Safety ?

Safety est un outil similaire a pip-audit mais utilise sa propre base de donnees de vulnerabilites.

### Lancer le scan

```bash
# Scan de l'environnement
safety scan

# Scan depuis requirements.txt
safety check -r requirements.txt
```

### Pourquoi utiliser les deux (pip-audit ET safety) ?

Les deux outils utilisent des bases de donnees differentes. Un package peut etre signale par l'un mais pas l'autre. Utiliser les deux maximise la couverture.

---

## Etape 4 : Revue manuelle du code

Les outils automatises ne detectent pas tout. Voici la checklist de revue manuelle a suivre :

### 4.1 Injection SQL

Chercher les patterns dangereux dans le code :

```bash
# Trouver toutes les f-strings contenant des mots-cles SQL
# Chercher dans le code : SELECT, INSERT, UPDATE, DELETE avec f" ou .format(
grep -rn "f\".*SELECT\|f\".*INSERT\|f\".*UPDATE\|f\".*DELETE" app/
grep -rn "\.format.*SELECT\|\.format.*INSERT" app/
```

**Regle** : Toute valeur provenant de l'utilisateur (query param, body, header) ne doit JAMAIS etre injectee dans une requete SQL via f-string ou `.format()`. Utiliser les placeholders `?` ou `%s` du connecteur :

```python
# MAUVAIS (vulnerable)
query = f"SELECT * FROM table WHERE id = '{user_input}'"

# BON (parametre)
query = "SELECT * FROM table WHERE id = ?"
db.fetch_all(query, [user_input])
```

### 4.2 Authentification et autorisation

Verifier :
- [ ] Tous les endpoints sensibles ont un middleware d'authentification
- [ ] Les tokens/API keys sont valides et non expires
- [ ] Les roles/permissions sont verifies avant chaque action

### 4.3 Exposition d'information

Verifier :
- [ ] Les messages d'erreur ne contiennent pas de stacktrace en production
- [ ] Les requetes SQL ne sont pas renvoyees au client (`DEBUG_SHOW_QUERY=false`)
- [ ] Le endpoint `/health` ne divulgue pas trop d'information
- [ ] Les headers HTTP ne revelent pas la version du framework

### 4.4 Configuration Docker

Verifier :
- [ ] Le container ne tourne pas en root (`USER` directive)
- [ ] Un `HEALTHCHECK` est defini
- [ ] Les ports ne sont exposes que sur les interfaces necessaires
- [ ] Les limites de ressources sont configurees dans docker-compose
- [ ] `.dockerignore` exclut `.env`, `.git`, `logs/`

### 4.5 Gestion des secrets

Verifier :
- [ ] `.env` est dans `.gitignore`
- [ ] Aucun secret n'est hardcode dans le code source
- [ ] Les valeurs par defaut dans `config.py` ne sont pas des vrais secrets
- [ ] Les secrets ne sont pas logges (verifier les appels `logger.*`)

---

## Etape 5 : Analyse de performance

### 5.1 Identifier les requetes lentes

Ajouter temporairement un log de duree sur chaque requete :

```python
import time
start = time.perf_counter()
result = databricks.fetch_all(query)
duration = time.perf_counter() - start
logger.info("Query took %.3fs: %s", duration, query[:100])
```

> Note : Ce pattern est deja en place dans les routes (`execution_time_s` dans les reponses).

### 5.2 Verifier la pagination

Tester avec des grands jeux de donnees :

```bash
# Verifier que page 1 et page 2 ne retournent pas les memes donnees
curl -X POST http://localhost:8000/trafics/get_trafics_paginated \
  -H "Content-Type: application/json" \
  -d '{"periode":"auto","co_regate":"XXX","date_debut":"20240101","date_fin":"20240630","page":1,"page_size":10}'

curl -X POST http://localhost:8000/trafics/get_trafics_paginated \
  -H "Content-Type: application/json" \
  -d '{"periode":"auto","co_regate":"XXX","date_debut":"20240101","date_fin":"20240630","page":2,"page_size":10}'
```

### 5.3 Verifier les timeouts

```bash
# Tester une requete avec une large plage de dates (proche de la limite 730 jours)
curl -X POST http://localhost:8000/trafics/get_trafics \
  -H "Content-Type: application/json" \
  -d '{"periode":"auto","co_regate":"XXX","date_debut":"20230101","date_fin":"20241231"}'
```

Si la requete depasse 30s, un timeout devrait etre configure.

---

## Etape 6 : Automatiser en CI/CD

Creer un fichier `.github/workflows/security.yml` (ou equivalent GitLab) :

```yaml
name: Security Audit

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 8 * * 1'  # Chaque lundi a 8h

jobs:
  security:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Bandit - Analyse statique
        run: bandit -r app/ -ll --exit-zero -f json -o bandit-report.json

      - name: pip-audit - Vulnerabilites dependances
        run: pip-audit -r requirements.txt --strict

      - name: Upload rapport Bandit
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: bandit-report
          path: bandit-report.json
```

---

## Etape 7 : Suivi et frequence

| Action | Frequence | Responsable |
|--------|-----------|-------------|
| `bandit -r app/` | A chaque PR (CI) | Automatique |
| `pip-audit -r requirements.txt` | A chaque PR (CI) + hebdomadaire | Automatique |
| `safety scan` | Hebdomadaire | Automatique |
| Revue manuelle (checklist etape 4) | A chaque nouvelle route/endpoint | Developpeur |
| Mise a jour des dependances | Mensuelle | Developpeur |
| Revue complete (ce guide) | Trimestrielle | Equipe |

---

## Annexe : Commandes rapides

```bash
# Audit complet en une seule commande
echo "=== BANDIT ===" && bandit -r app/ -ll && echo "=== PIP-AUDIT ===" && pip-audit -r requirements.txt && echo "=== SAFETY ===" && safety scan

# Generer un rapport JSON complet
bandit -r app/ -f json -o rapport_bandit.json
pip-audit -r requirements.txt -f json -o rapport_pip_audit.json
```

---

*Derniere mise a jour : 2026-04-08*
