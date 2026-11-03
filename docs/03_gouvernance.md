# Gouvernance des données (S1 p26, data mesh S1 p25)

## 1. Ownership (domain ownership)

- **Data owner :** l'équipe « prévision court terme » possède `gold/jeu_prevision` et décide de son contrat.
- **Data steward :** veille quotidienne sur la qualité (taux de validité, réconciliation par batch).
- Chaîne **autonome** de la source au résultat, sans dépendance à une autre équipe (data mesh).

## 2. Catalog et glossaire

Termes définis une seule fois, utilisés partout :

| Terme | Définition |
|---|---|
| **jour climatologique** | journée calendaire UTC (l'API livre des extrêmes journaliers tn/tx) |
| **origine J** | date à laquelle on prévoit ; on connaît toutes les observations ≤ J |
| **horizon h** | nombre de jours entre l'origine et la cible (J+h, h ∈ 1..7) |
| **climatologie** | moyenne lissée (±15 j) par station et jour de l'année, apprise sur *train* |
| **anomalie** | valeur observée moins sa climatologie |
| **split** | partition d'usage : train / valid / test / purge |

## 3. Data quality — règles, seuils, SLA (ODCS `quality`, S2 p28)

| Code | Contrôle | Seuil / action |
|---|---|---|
| Q1 | plage physique | tn ∈ [−35 ; 35], tx ∈ [−25 ; 47] °C sinon INVALIDE → quarantaine |
| Q2 | cohérence interne | tn ≤ tx sinon INVALIDE (les deux marquées) |
| Q3 | complétude | jour absent du calendrier → marqué |
| Q4 | cohérence spatiale | |score robuste vs autres stations| > 10 → INVALIDE (capteur) |
| Q5 | précipitations | rr ∈ [0 ; 1000] mm sinon rr = null |
| Q6 | référentiel | station absente du référentiel → quarantaine |

**SLA produit :** `taux_qc_valide ≥ 95 %` par station (actuellement ≈ 99,97 % sur les 15),
Gold publié à J+1. Règles portées par les **data contracts ODCS** (`contracts/`) et **testées en CI**
(`check_contracts.py` + pytest).

## 4. Access control

- `raw/` et `ref/` en **lecture seule** (immuables).
- Moindre privilège : l'apprentissage IA lit Gold, jamais Bronze.
- Journalisation des exécutions via le batch log et les events OpenLineage.

## 5. Conformité et licences

Source **Infoclimat / Météo-France**, **Licence Ouverte Etalab 2.0** : attribution obligatoire,
réutilisation libre. L'attribution est conservée dans le manifeste d'acquisition et le data contract Bronze.

## 6. Lifecycle (rétention)

| Zone | Rétention |
|---|---|
| `raw/` | conservée indéfiniment (source de vérité pour le replay) |
| `bronze/` | conservée (reconstructible depuis `raw/`) |
| `silver/`, `gold/` | reconstructibles → supprimables et recalculables |
| `_meta/` (batch log, lineage) | conservé (audit) |
| fichiers temporaires de recovery (`*_tmp/`) | purgés après commit |

## 7. Évolution des contrats (semver, S1 p35)

| Changement | Type | Version |
|---|---|---|
| ajouter une colonne optionnelle | compatible | 1.0 → 1.1 |
| changer une unité, un grain, une clé | cassant | 1.x → 2.0 |
| supprimer / renommer une colonne | cassant | 1.x → 2.0 |
