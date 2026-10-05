# Projet météo ESGI — Prévision à court terme

Prévoir les températures journalières (tn, tx) d'une station à J+1 … J+7, et montrer que le modèle
bat deux références naïves : la **persistance** et la **climatologie saisonnière**, sur une période future réservée.

Source : API ouverte Infoclimat — https://portail.chom.engineering (doc) · https://climato.chom.engineering (API)

## Architecture

| Couche | Contenu | Script |
|---|---|---|
| Bronze | Réponses JSON brutes de l'API + manifeste | `src/ingest_bronze.py` ✅ |
| Silver | Table propre station × jour, calendrier complet, flags de qualité | `src/build_silver.py` ✅ |
| Gold | Variables retardées (lags), saisonnalité, cibles J+h, découpage chronologique | `src/build_gold.py` ✅ |
| IA | Ridge + LightGBM, un modèle par variable et horizon | `src/train_models.py` ✅ |
| Évaluation | MAE/RMSE par horizon vs persistance et climatologie | `src/plot_results.py` ✅ |

Les data contracts (format ODCS) sont dans `contracts/`.

## Lancer l'étape 1 (Bronze)

```bash
cd ~/Projects/projet_meteo_esgi
python3 src/ingest_bronze.py
```

Durée : 1 à 2 minutes. Pas de dépendance à installer.
Options : `--from 2010-01-01`, `--to 2026-01-01`, `--stations 07149 07630`.

Résultat dans `data/bronze/` :

- `ref/stations_<date>.json` : référentiel des stations
- `v2_journaliere/<ic_id>/page_XXX.json` : séries brutes, une page = 20 000 lignes maximum
- `_manifest_<date>.json` : traçabilité (date, paramètres, lignes par station)

## Lancer l'étape 2 (Silver)

```bash
python3 src/build_silver.py
```

Sortie : `data/silver/meteo_journaliere.parquet` (ou `.csv` si pyarrow est absent) et `data/silver/rapport_qualite.csv`.

Règles qualité : jour absent, valeurs hors bornes physiques, tn > tx, et incohérence spatiale
(anomalie de la station très différente de celle des autres stations le même jour, seuil 10 écarts robustes).
Le seuil a été calibré : à 6, de vrais événements (canicule du 19/07/2022 à Orly, froid de décembre 2009 à Dijon)
étaient rejetés à tort. À 10, seule l'erreur de capteur de Lille (48,7 °C le 27/05/2005, 46,9 °C le 28/05/2005) est écartée.

## Lancer l'étape 3 (Gold)

```bash
python3 src/build_gold.py
```

Sortie : `data/gold/jeu_prevision.parquet` (une ligne par station × date d'origine J), `climatologie_train.parquet`, `dictionnaire_variables.csv`.

- À la date J on connaît tout jusqu'à J inclus ; on prévoit tn et tx de J+1 à J+7.
- 79 variables : valeurs de J, décalages (J-1 … J-7, J-14, J-21, J-365), moyennes glissantes 3/7/14/30 j, anomalies, pluie, saison, anomalie nationale.
- Découpage : train ≤ 2021, valid 2022-2023, test ≥ 2024. Les 7 jours avant chaque frontière sont en `purge` (leurs cibles déborderaient sur la période suivante).
- Contrôles anti-fuite passés : variables inchangées si on supprime le futur, cibles exactes, climatologie apprise sur train seul.

Références sur valid (MAE de tx) : persistance 2,08 °C à J+1, 3,10 °C à J+3, 3,70 °C à J+7 ; climatologie ≈ 3,1 °C à tout horizon.

## Lancer l'étape 4 (modèles et évaluation)

```bash
pip install lightgbm scikit-learn matplotlib joblib pyarrow
python3 src/train_models.py      # environ 7 min ; reprenable (résultats partiels dans results/_partiel)
python3 src/plot_results.py
```

Protocole : hyperparamètres choisis sur valid (alpha de Ridge, nombre de feuilles et d'arbres de LightGBM),
réentraînement sur train + valid, puis évaluation unique sur test (2024 → sept. 2026).

Résultats sur test (MAE, °C) :

| | tx J+1 | tx J+3 | tx J+7 | tn J+1 | tn J+3 | tn J+7 |
|---|---|---|---|---|---|---|
| Persistance | 2,22 | 3,32 | 4,01 | 2,00 | 2,94 | 3,38 |
| Climatologie | 3,21 | 3,21 | 3,21 | 2,67 | 2,67 | 2,67 |
| Ridge | 2,04 | 2,78 | 3,07 | 1,68 | 2,28 | 2,55 |
| **LightGBM** | **1,96** | **2,78** | **3,06** | **1,62** | 2,30 | 2,57 |

Le meilleur modèle bat la meilleure référence à tous les horizons, de 12 à 22 % à J+1-J+2 et de 4 à 5 % à J+7,
et sur les 15 stations. Détails : `results/metriques_test.csv`, `results/metriques_station.csv`, graphiques dans `results/`.

## Tests et contrôles qualité

```bash
pip install -r requirements.txt
python3 src/check_contracts.py   # applique les data contracts ODCS aux tables réelles
python3 -m pytest                # 27 tests, environ 10 s
./run_pipeline.sh                # tout le pipeline, de l'API aux graphiques, puis les tests
```

`src/check_contracts.py` lit chaque contrat de `contracts/` et vérifie sur la table réelle (via DuckDB) :
les colonnes déclarées, les champs obligatoires, la clé primaire et chaque règle `quality` de type SQL.
Les règles de type `custom` (anti-fuite du Gold) sont vérifiées par pytest.

| Fichier | Ce qui est testé |
|---|---|
| `tests/test_contracts.py` | Les 3 contrats sont respectés ; le vérificateur détecte bien une table corrompue |
| `tests/test_silver_regles.py` | Données synthétiques : pic de capteur rejeté, canicule générale conservée, hors bornes, tn > tx, manquants, pluie invalide, journée partielle |
| `tests/test_gold_fuite.py` | Variables indépendantes du futur, cibles exactes, pas de débordement train/valid, climatologie apprise sur train seul |
| `tests/test_ingestion.py` | API simulée : format GeoJSON, alias de stations, pagination |

Les tests qui ont besoin des données sont ignorés tant que le pipeline n'a pas tourné.

## Notes sur la source

- L'API ouverte est **journalière**. Les données horaires demandent un jeton (`archive.chom.engineering`).
- Limite de 20 000 lignes par requête : le script suit la pagination (`tronque` / `suite.from`).
- Licence : Licence Ouverte Etalab 2.0 / politique opendata Infoclimat.
