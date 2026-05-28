# DSR-659 — MAJ complète du TMH recalculé d'un scénario

> **User story** : « En tant que TRPPU, je veux que les informations de trafic
> d'un scénario modifié puissent être mises à jour en base afin de disposer des
> trafics "recalculés" par produit pour le scénario modifié. »
>
> **Tickets liés** : **appelé par DSR-656** ; recouvre DSR-649 (qui est un
> sous-ensemble) ; symétrique de la lecture DSR-650.

---

## 1. Contexte & objectif métier

Service (yb04) qui met à jour (**UPDATE**) `trppu_tmh` pour chaque produit du
tableau TMH après recalcul. Paramètres par produit :
`id_scenario`, `co_produit`, `volume_realise`, `volume_previsionnel`,
`moyenne_journaliere`, `moyenne_hebdo`, `exclusion`.

Champs mis à jour : `volume_realise`, `volume_previsionnel`, `moyenne_journaliere`,
`moyenne_hebdo`, `dt_calcul` (= NOW()), `bl_exclu`.

> Différence avec **DSR-649** : DSR-659 met aussi à jour `volume_previsionnel` et
> `bl_exclu` (MAJ complète post-recalcul), là où DSR-649 ne touche que le volume
> réalisé + moyennes. Voir `README_ameliorations.md` (unification proposée).

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** → mutualisé dans `app/routes/trppu_tmh/` (avec DSR-649/650). |
| Table `trppu_tmh` | Existe, toutes colonnes présentes. |
| Ligne absente | Cas « produit pas encore en base » à arbitrer (UPDATE seul vs upsert). |

---

## 3. Spécification table `trppu_tmh` (UPDATE par produit)

```sql
UPDATE trppu_tmh
   SET volume_realise = %s,
       volume_previsionnel = %s,
       moyenne_journaliere = %s,
       moyenne_hebdo = %s,
       bl_exclu = %s,
       dt_calcul = NOW()
 WHERE id_scenario = %s AND co_produit = %s;
```
> Si `rowcount == 0` (ligne absente) : décider INSERT (upsert) ou erreur. Proposé :
> **upsert** pour robustesse — cf. `README_incomprehensions.md`.

---

## 4. Contrat d'API proposé

Deux usages :
- **autonome** : `PUT /trppu-api/scenarios/{id_scenario}/tmh` (batch tous produits) ;
- **intégré** : appelé par DSR-656 dans la même transaction (bloc `tmh[]`).

Body `PUT` (batch) :
```json
{
  "tmh": [
    { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
      "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00, "exclusion": false },
    { "co_produit": "IP", "volume_realise": 50000, "volume_previsionnel": 0,
      "moyenne_journaliere": 1666.67, "moyenne_hebdo": 10000.00, "exclusion": true }
  ]
}
```
Réponse `200` : `{ "id_scenario": 12, "nb_updated": 2, "nb_inserted": 0 }`.
Codes : `200` ; `404` scénario introuvable ; `409` figé.

---

## 5. Modèle Pydantic proposé

```python
class TmhUpsert(BaseModel):
    model_config = ConfigDict(extra="forbid")
    co_produit: str = Field(..., min_length=2, max_length=2)
    volume_realise: int | None = Field(None, ge=0)
    volume_previsionnel: int | None = Field(None, ge=0)
    moyenne_journaliere: Decimal = Field(..., max_digits=12, decimal_places=2)
    moyenne_hebdo: Decimal = Field(..., max_digits=12, decimal_places=2)
    exclusion: bool                 # -> bl_exclu

class TmhBatchUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tmh: list[TmhUpsert] = Field(..., min_length=1)
```

---

## 6. DDL de migration

Aucune.

---

## 7. Logging

Tracer `id_scenario`, nb de produits reçus, nb mis à jour / insérés. id session IHM si fourni.

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Après MAJ scénario + actualisation, `trppu_tmh` correct et en phase IHM | §3-§4 |

## 9. Questions ouvertes

`README_incomprehensions.md` : 1 appel/produit vs batch ; UPDATE seul vs upsert si
ligne absente ; calcul des moyennes (IHM vs serveur) ; **chevauchement DSR-649/659**.
