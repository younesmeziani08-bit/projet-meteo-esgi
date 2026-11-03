# Schéma d'architecture

Lakehouse en **batchs incrémentaux** (S1 p10), médaillon Bronze → Silver → Gold, calcul **PySpark**
(local `local[N]` pour la démonstration distribuée), stockage **Parquet partitionné**.

## 1. Vue d'ensemble

```
                    ┌─────────────────────────────────────────────────────────────┐
  API Infoclimat    │                     data/ (object storage local)            │
  climato.chom…  ──▶ │                                                             │
  (acquisition)     │  raw/            zone brute IMMUABLE + checksum SHA-256      │
                    │   source=infoclimat/batch=<id>/ref|v2_journaliere/*.json    │
                    │        │  (replay lit ICI, jamais l'API → reproductible)     │
                    │        ▼                                                      │
                    │  bronze/obs/_batch_id=<id>/   Parquet, champs TEXTE,          │
                    │        │                      colonnes techniques            │
                    │        ▼                                                      │
                    │  silver/observations/annee=/mois=/   schéma canonique,       │
                    │  silver/quarantaine/annee=/mois=/    qc_flag, version, dedup  │
                    │        │                                                      │
                    │        ▼                                                      │
                    │  gold/jeu_prevision/split=/   features + cibles J+1..J+7      │
                    │  gold/climatologie_train/     baseline saisonnière           │
                    │                                                               │
                    │  _meta/batchs.parquet   batch log   ·   _meta/lineage/  OpenLineage │
                    └─────────────────────────────────────────────────────────────┘
                             │                       │
                             ▼                       ▼
                    IA (Ridge, LightGBM)      Spark UI / event log → mesures (speedup, vCPU·h)
                    évaluation vs baselines
```

## 2. Rôle et partitioning de chaque table (S1 p16, S2 p30, S3 p9)

| Table | Grain | Partitionnement | Justifié par |
|---|---|---|---|
| `raw/` | fichier source tel que reçu | `source=` / `batch=` | Immutabilité, replay par batch, checksum |
| `bronze/obs` | une ligne source (texte brut) | `_batch_id=` | Ingestion/relance idempotente par batch (overwrite dynamique) |
| `silver/observations` | station × jour (schéma canonique) | `annee=` / `mois=` | Nouveaux batchs et recalculs ne touchent que quelques dossiers ; tri interne par station+date pour min/max sélectifs |
| `silver/quarantaine` | ligne rejetée + règle violée | `annee=` / `mois=` | Revue et replay des rejets |
| `gold/jeu_prevision` | station × date d'origine J | `split=` (train/valid/test/purge) | Lecture directe par usage (apprentissage lit train+valid, éval lit test) |

**Stockage ≠ calcul (S1 p43) :** Silver est *stocké* par date ; les calculs à fenêtre glissante
(features Gold) *repartitionnent par station* le temps du calcul (window `partitionBy(station)`).

## 3. Où sont les shuffles (S3 p7)

| Traitement | Opération Spark | Shuffle ? | Mitigation |
|---|---|---|---|
| Bronze : ajout colonnes techniques | `withColumn`, `lit` | Non (narrow) | — |
| Silver : join référentiel stations | `join(broadcast(ref))` | **Non** | Référentiel ~15 lignes → broadcast |
| Silver : déduplication déterministe | `Window.partitionBy(clé) + row_number` | **Oui** (complet) | Fait **une seule fois**, en Silver |
| Silver : qualité spatiale (anomalie vs autres stations/jour) | `groupBy(date)` | Oui, réduit | Pré-agrégation |
| Gold : lags, moyennes glissantes | `Window.partitionBy(station).orderBy(date)` | **Oui** (par station) | Lookback borné ; `rangeBetween(-k, 0)` jamais centré (anti-fuite) |
| Gold : anomalie nationale | `Window.partitionBy(date)` | Oui, réduit | Pré-agrégation |

## 4. Frontières de partition et lookback (S3 p9, p13)

Les moyennes glissantes (3/7/14/30 j) et les lags (jusqu'à J-365) du jour *début de partition*
ont besoin des jours de la **partition précédente**. Règle incrémentale :
**lire période + lookback → calculer → publier la période seule** ; le lookback est compté dans le coût.
Côté cibles J+h, un **lookahead de 7 jours** est nécessaire côté aval → zones de **purge** aux frontières
train/valid/test (déjà en place).

## 5. Anti-data-leakage (S3 p10) — point fort du projet

- **Temporal leakage :** toutes les features n'utilisent que des données ≤ J (`rangeBetween(-k, 0)`),
  jamais une fenêtre centrée.
- **Time-based split :** train ≤ 2021 · valid 2022-2023 · test ≥ 2024, **versionné dans la table** (`split=`).
- **Climatologie apprise sur *train* seul**, jamais recalculée avec valid/test.
- **Purge** de 7 jours aux frontières pour empêcher les cibles de déborder sur la période suivante.

## 6. Environnement de calcul

Démonstration locale : une seule JVM Spark, parallélisme réglé par `local[N]` (N = cœurs = « workers »
logiques), snapshot et commit figés entre deux mesures. Transposable au cluster du cours (driver +
8 workers r3-32) sans changement de code applicatif : seules changent `master` et les partitions.
