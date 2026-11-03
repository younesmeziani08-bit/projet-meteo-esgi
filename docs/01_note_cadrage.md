# Note de cadrage — Prévision météo à court terme

**Équipe / domaine :** prévision de température (variante *Prévision à court terme*, S1 p22)
**Version :** 1.0 · **Dates de référence :** données 2000-01-01 → 2026-09-29, 15 stations synoptiques

---

## 1. Besoin métier

Prévoir, pour une station donnée et à la date d'origine **J**, les températures **minimale (tn)** et
**maximale (tx)** des jours **J+1 à J+7**, et **démontrer un gain mesuré** face à deux méthodes simples.

- **Consommateur identifié :** un service d'aide à la décision (ex. alerte chaud/froid à 7 jours) qui
  lit le produit Gold pour entraîner et servir un modèle. Grain et fraîcheur fixés par ce consommateur.
- **Fraîcheur :** le Gold du jour J est disponible à J+1 (les observations de J sont connues).
- **Confiance :** traçabilité complète (lineage Bronze→Silver→Gold), reproductibilité bit-à-bit,
  contrôles qualité versionnés en data contracts testés en CI.

## 2. Périmètre (volontairement restreint au besoin du Gold, S1 p36 « hors périmètre »)

| Dimension | Choix | Justification |
|---|---|---|
| Stations | 15 postes synoptiques répartis sur la métropole | Couverture climatique variée, volume maîtrisable, pas de skew extrême |
| Variables | tn, tx, tm, rr (pluie) | tn/tx sont les cibles ; tm et rr nourrissent les features. Vent/pression hors périmètre |
| Période | 2000 → aujourd'hui (~9 769 j/station) | Historique suffisant pour la saisonnalité et des lags J-365 |
| Pas de temps | journalier | L'API ouverte Infoclimat est journalière (l'horaire exige un jeton) |

## 3. Produit Gold (S1 p27 : résultat / baseline / validation)

- **Résultat attendu :** une ligne par **station × date d'origine J**, avec ~79 variables d'entrée
  (toutes ≤ J), les cibles `cible_{tn,tx}_h{1..7}`, et les baselines précalculées.
- **Baselines à battre :**
  1. **Persistance** — « demain = aujourd'hui » : `tx(J)` sert de prévision à tout horizon.
  2. **Climatologie saisonnière** — moyenne du jour de l'année (apprise sur *train* uniquement).
- **Critères de validation (fixés AVANT de mesurer) :**
  - Correction : sur un sous-ensemble contrôlé, le Gold se recalcule à l'identique (écart nul ou expliqué).
  - Anti-fuite : variables inchangées si on supprime le futur ; cibles exactes ; climatologie sur *train* seul.
  - IA : MAE (test réservé ≥ 2024) **strictement inférieure** à la meilleure baseline, à chaque horizon.

## 4. Usage IA et résultat de référence

Un modèle par variable × horizon (2 × 7 = 14), deux familles : **Ridge** (linéaire régularisé) et
**LightGBM**. Protocole sans fuite : réglage des hyperparamètres sur *valid* (2022-2023),
réentraînement sur *train+valid*, **évaluation unique sur test** (2024 → sept. 2026).

**Résultat de référence obtenu (MAE °C sur test, n ≈ 15 000/horizon) :**

| | tx J+1 | tx J+3 | tx J+7 | tn J+1 | tn J+3 | tn J+7 |
|---|---|---|---|---|---|---|
| Persistance | 2,22 | 3,32 | 4,01 | 2,00 | 2,94 | 3,38 |
| Climatologie | 3,21 | 3,21 | 3,21 | 2,67 | 2,67 | 2,67 |
| **LightGBM** | **1,96** | **2,78** | **3,06** | **1,62** | 2,30 | 2,57 |

Gain de LightGBM vs meilleure baseline : **+11,5 %** (tx J+1), **+13,5 %** (tx J+3), **+4,7 %** (tx J+7) —
positif à tous les horizons et sur les 15 stations.

## 5. Critères de réussite (ce que le jury vérifie, S1 p4)

1. **Correction** — résultats conformes à la référence sur le sous-ensemble contrôlé.
2. **Distribué justifié** — mesures 1 worker vs N workers, speedup et coût (voir `04_tableau_experiences.md`).
3. **Optimisation démontrée** — version initiale (pandas, étalon) vs optimisée (Spark), à sortie identique.
4. **Exploitation** — nouveau batch incrémental, correction historique (backfill), recovery après incident.
5. **IA évaluée** — gain significatif et reproductible face à la baseline (tableau §4).

## 6. Hors périmètre

Vent, pression, neige ; pas de temps horaire ; stations hors des 15 retenues ; toute règle qualité ou
variable qui ne sert pas directement le produit Gold de prévision.
