# Execution plan annoté — Gold Spark (livrable bloc 1, S3 p21)

Plan complet : [`execution_plan_gold.txt`](execution_plan_gold.txt) (`gold.explain("formatted")`).
Obtenu par `python3 src/spark/build_gold_spark.py --plan`.

## 1. Lecture du plan physique

Le plan exécuté (AQE) est une pile de `Window → Project → BroadcastHashJoin`, soit :

| Nœud | Nombre | Lecture |
|---|---|---|
| **BroadcastHashJoin** | 16 | ✅ jointures climatologie / référentiel / `base_clim_*_h{1..7}` : **aucun shuffle** du gros côté (la petite table de climatologie, ~5 475 lignes, est diffusée) |
| **Window** | 10 | lags, moyennes glissantes, cibles (par station) + anomalie nationale (par date) |
| **Exchange** (shuffles) | ≈ 59 | repartitionnements : essentiellement un par bloc de `Window`, voir §2 |
| **SortMergeJoin** | 9 | construction de la table de climatologie (jointures `outer` tn×tx non diffusées) — sur petit volume |
| **HashAggregate** | 72 | moyennes par (ic_id, doy), médianes, anomalie nationale (pré-agrégations) |

## 2. Où sont les shuffles, et pourquoi (S3 p7)

- Les **fenêtres par station** (`partitionBy(ic_id)`) partagent la même clé : Spark peut réutiliser un
  même repartitionnement pour les lags, moyennes et cibles **tant qu'elles se suivent**.
- L'**anomalie nationale** utilise `partitionBy(date)` : clé différente → un shuffle supplémentaire.
  Comme elle est intercalée entre des fenêtres par station, Spark **re-shuffle par station** ensuite.
- Les jointures `broadcast` n'introduisent **aucun** shuffle (vérifié : `BroadcastHashJoin`).

**→ Piste d'optimisation (Phase 7, levier O2) :** regrouper tous les calculs `partitionBy(ic_id)`
d'un seul tenant et isoler l'anomalie nationale, pour ne payer qu'**un** repartitionnement par station.
Levier O3 : diffuser (`broadcast`) les jointures de la table de climatologie pour remplacer les 9
`SortMergeJoin` par des `BroadcastHashJoin`.

## 3. Pushdown (S3 p8)

Le scan Parquet de Silver pousse la projection (`ReadSchema` limité aux colonnes utiles). La table Gold
est ensuite partitionnée par `split`, ce qui permet au consommateur IA un **partition pruning**
(l'apprentissage lit `split=train`/`valid`, l'évaluation lit `split=test`).

## 4. Correction (critère jury n°1)

Sortie Spark comparée à la référence pandas sur un échantillon représentatif de 18 colonnes
(lags, moyennes/écarts glissants, anomalies, anomalie nationale, climatologie, cibles, baselines) :
**écart maximal 0,00 — identique au bit près**, et tailles de split identiques
(train 120 435 · valid 10 845 · test 15 045 · purge 210). Vérification exhaustive : Phase 6.
