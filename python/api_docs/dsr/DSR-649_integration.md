# DSR-649 — MAJ du TMH suite à modification d'un trafic initial

> **User story** : « En tant que TRPPU, je veux que les informations de trafic
> d'un scénario soient mises à jour en base en cas de modification des trafics
> initiaux afin d'actualiser les trafics moyen hebdo des produits modifiés. »
>
> **Tickets liés** : DSR-659 (MAJ TMH **complète** après recalcul — recouvre ce
> ticket), DSR-650 (lecture TMH), DSR-634 (création initiale des lignes TMH).

---

## 1. Contexte & objectif métier

Quand l'utilisateur **modifie un trafic dans le tableau TMH de l'IHM**, un service
(yb04) met à jour la **ligne existante** de `trppu_tmh` du couple
(`id_scenario`, `co_produit`).

Paramètres reçus : `id_scenario`, `co_produit`, **nouveau volume**, `moyenne_journaliere`, `moyenne_hebdo`.

Champs mis à jour : `volume_realise`, `moyenne_journaliere`, `moyenne_hebdo`, `dt_calcul` (= NOW()).

> Ce ticket est un **UPDATE ciblé**. Il ne touche **ni** `volume_previsionnel`
> **ni** `bl_exclu` (contrairement à DSR-659 qui fait la MAJ complète après
> recalcul). Voir `README_ameliorations.md` (proposition d'unifier 649 + 659).

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** — aucun endpoint TMH. À créer (`app/routes/trppu_tmh/`). |
| Table `trppu_tmh` | Existe : `id_tmh, id_scenario, co_produit, volume_realise, volume_previsionnel, moyenne_journaliere, moyenne_hebdo, dt_calcul, bl_exclu`. |
| Pré-requis | La ligne TMH doit déjà exister (créée à DSR-634). Comportement si absente → cf. §8. |

---

## 3. Spécification table `trppu_tmh` (UPDATE)

```sql
UPDATE trppu_tmh
   SET volume_realise = %s,
       moyenne_journaliere = %s,
       moyenne_hebdo = %s,
       dt_calcul = NOW()
 WHERE id_scenario = %s AND co_produit = %s;
```

| Colonne | Mise à jour ? |
| ------- | :-----------: |
| `volume_realise` | ✅ (nouveau volume) |
| `moyenne_journaliere` | ✅ |
| `moyenne_hebdo` | ✅ |
| `dt_calcul` | ✅ (NOW) |
| `volume_previsionnel` | ❌ inchangé |
| `bl_exclu` | ❌ inchangé |

---

## 4. Contrat d'API proposé

`PATCH /trppu-api/scenarios/{id_scenario}/tmh/{co_produit}`

Body :
```json
{ "volume_realise": 120000, "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00 }
```
Codes : `200` ligne mise à jour ; `404` ligne TMH (scénario+produit) introuvable ;
`409` scénario figé.

---

## 5. Modèles Pydantic proposés

```python
class TmhVolumeUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    volume_realise: int = Field(..., ge=0)
    moyenne_journaliere: Decimal = Field(..., max_digits=12, decimal_places=2)
    moyenne_hebdo: Decimal = Field(..., max_digits=12, decimal_places=2)

class TmhOut(BaseModel):              # partagé avec DSR-650
    model_config = ConfigDict(from_attributes=True)
    co_produit: str
    volume_realise: int | None
    volume_previsionnel: int | None
    moyenne_journaliere: Decimal
    moyenne_hebdo: Decimal
    bl_exclu: bool
```

---

## 6. DDL de migration

Aucune (la table `trppu_tmh` possède déjà toutes les colonnes).

---

## 7. Logging

Tracer `id_scenario`, `co_produit`, anciens/nouveaux volumes (via `safe_preview`),
`rowcount` de l'UPDATE pour détecter le cas « 0 ligne modifiée ».

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Après MAJ tableau IHM, `trppu_tmh` correct pour les produits modifiés | §3 + `PATCH` §4 |
| Données en phase avec l'IHM | moyennes reçues telles quelles (calculées IHM) |

## 9. Questions ouvertes

`README_incomprehensions.md` : qui calcule les moyennes (IHM vs serveur) ?
comportement si la ligne TMH n'existe pas (404 vs upsert) ? recalculs en cascade
(trafic agrebal/pdi) hors scope ? **chevauchement DSR-649 / DSR-659**.
