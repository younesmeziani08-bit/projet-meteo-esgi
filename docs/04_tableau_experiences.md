# Tableau d'expériences (protocole, S1 p48 — S3 p26/p29)

Protocole de mesure : même snapshot, même commit, même config Spark ; **une seule chose varie** par essai ;
**3 exécutions minimum**, médiane et écart rapportés ; tout compté (agrégats intermédiaires et lookback inclus).
Les mesures sont collectées par script depuis l'historique Spark (`/api/v1/applications/{id}/stages`) et
archivées avec le dossier. **Chiffres remplis par `bench/run_bench.py`.**

## 1. Essais prévus

| Essai | Hypothèse | Configuration | Mesures | Critère de réussite |
|---|---|---|---|---|
| **Correction** | Gold = référence | sous-ensemble contrôlé | écarts par station | 0 écart non expliqué |
| **Nouveau batch** | seules les partitions touchées sont recalculées | batch « late data » | partitions réécrites, durée | résultat = recalcul complet |
| **Recovery** | pas de double comptage | arrêt forcé à mi-calcul | lignes, sommes de contrôle | identique à une exécution sans arrêt |
| **Performance** | N workers tiennent le délai | 1, 2, 4, 8 workers | durée, shuffle, mémoire, vCPU·h | délai respecté, speedup rapporté |
| **IA** | gain vs référence | période future réservée (test ≥ 2024) | MAE par horizon | gain significatif (✓ voir note de cadrage) |

## 2. Performance 1 / N workers (S3 p29) — mesuré

Mesures : `python3 bench/run_bench.py --workers 1 2 4 8 --reps 3` → `results/bench.csv`,
`results/bench_resume.csv`, courbe `results/speedup.png`. Build complet du Gold Spark (15 stations,
146 535 lignes), `shuffle.partitions=64` fixe, AQE activé, médiane sur 3 répétitions.

| Essai | Version | Workers | Durée méd. (s) | Shuffle read (Mo) | Spill mém. (Mo) | vCPU·h | Speedup | Efficiency | Sortie = réf. ? |
|---|---|---|---|---|---|---|---|---|---|
| E1 | initiale | 1 | 80,1 | 108,1 | 104,9 | 0,022 | ×1,00 | 1,00 | ✓ |
| E2 | initiale | 2 | 48,3 | 110,1 | 0,0 | 0,027 | ×1,66 | 0,83 | ✓ |
| E3 | initiale | 4 | 31,6 | 112,4 | 0,0 | 0,035 | ×2,53 | 0,63 | ✓ |
| E4 | initiale | 8 | 29,8 | 114,4 | 0,0 | 0,066 | ×2,68 | 0,34 | ✓ |
| E5 | optimisée | 1 | 70,0 | 91,4 | 0,0 | 0,019 | ×1,00 | 1,00 | ✓ |
| E6 | optimisée | 8 | 26,7 | 91,4 | 0,0 | 0,059 | ×2,62 | 0,33 | ✓ |

**Speedup** S(n) = t₁ ÷ tₙ · **Efficiency** E(n) = S(n) ÷ n · **Coût** = workers × durée (vCPU·h). Voir `speedup.png`.

## 3. Optimisation testée (S3 p24, un levier à la fois) — mesuré

| # | Levier | Famille | Attendu | Mesuré (1 worker, E1→E5) |
|---|---|---|---|---|
| O1 | broadcast du référentiel stations et de la climatologie | échanger moins | pas de shuffle sur ces joins | ✓ `BroadcastHashJoin` dans le plan |
| O2 | regrouper les fenêtres `partitionBy(ic_id)`, isoler l'anomalie nationale | échanger moins | moins de shuffle, moins de spill | shuffle 108→**91 Mo**, spill **105→0 Mo**, durée 80→**70 s** |
| O3 | `inner join` broadcastable pour la climatologie (au lieu de full-outer SMJ) | échanger moins | `SortMergeJoin`→`BroadcastHashJoin` | ✓ plan, warning hint supprimé |
| O4 | AQE activé (coalesce auto des partitions de shuffle) | mieux répartir | tasks équilibrées | activé sur tous les essais |

**Test d'équivalence (S3 p23) :** `src/spark/equivalence.py` + `tests/test_equivalence.py` → Silver et Gold
**identiques** à la référence pandas (tolérance 1e-6), y compris après optimisation. Référence = pipeline
**pandas** d'origine (`src/*.py`), conservé comme étalon.

## 3bis. Lecture des résultats (justification distribué, S3 p40, loi d'Amdahl p27)

- **Speedup réel** : ×1,66 (2w), ×2,53 (4w), ×2,68 (8w). L'efficiency tombe sous 50 % dès 8 workers
  (0,34) : au-delà de **4 workers**, les workers ajoutés coûtent plus qu'ils ne rapportent — plateau d'Amdahl
  dû à la part séquentielle (lecture, construction du plan, écriture) et au faible volume (15 stations).
- **Coût** : la config la moins coûteuse qui respecte un délai donné est **1-2 workers** ; 8 workers triplent
  le coût en vCPU·h pour un gain marginal. *La bonne configuration est la moins coûteuse qui tient le délai.*
- **Une machine suffit-elle ?** Sur ce périmètre de test, oui (80 s sur 1 worker). Conformément à S3 p40,
  l'expérience distribuée est tout de même menée et mesurée ; elle devient nécessaire au volume réel du
  portail (39 Md de mesures, des milliers de stations), où le single-worker dépasserait tout délai.
- **Optimisation à qualité égale** : −12 % de durée et **spill éliminé** à 1 worker, sortie strictement
  identique (équivalence vérifiée) — exactement le critère « version initiale vs optimisée » du jury.

## 4. Exploitation (critère jury n°4) — vérifié

Démonstration : `python3 src/spark/exploitation.py` · tests : `tests/test_exploitation.py` (3 invariants).

| Scénario | Ce qui se passe | À recalculer | Perte / double comptage | Invariant vérifié |
|---|---|---|---|---|
| Doublon de batch (même checksum) | batch ignoré (idempotence) ; partition réécrite à l'identique | rien | aucun | ✓ lignes 3→3, rejeu ignoré |
| Correction historique (version 2) | la version récente gagne la dedup déterministe | partitions annee/mois touchées + lookback | ancienne valeur tracée en Bronze | ✓ gagnant v2, 1 ligne/clé |
| Perte d'un worker à mi-calcul | relance ne refait que les partitions non publiées | partitions non publiées seules | aucun (commit atomique, `src/spark/recovery.py`) | ✓ 2 publiées, 1 refaite, total exact |
| Station volumineuse (skew) | une task longue bloque le stage | — | — | AQE activé (coalesce/skew join) |
