"""Règles qualité Silver, testées sur des données synthétiques maîtrisées."""
import numpy as np
import pandas as pd
import pytest

import build_silver as bs

STATIONS = ["A", "B", "C", "D", "E", "F"]


@pytest.fixture(scope="module")
def jeu():
    """6 stations, 3 ans, saisonnalité + bruit, et des cas pièges injectés."""
    rng = np.random.default_rng(0)
    dates = pd.date_range("2020-01-01", "2022-12-31", freq="D")
    parts = []
    for i, s in enumerate(STATIONS):
        saison = 12 + 8 * np.sin(2 * np.pi * (dates.dayofyear - 110) / 365.25)
        commun = rng.normal(0, 2, len(dates)) if i == 0 else parts[0]["_commun"].to_numpy()
        tn = saison - 5 + commun + rng.normal(0, 1, len(dates))
        tx = saison + 5 + commun + rng.normal(0, 1, len(dates))
        parts.append(pd.DataFrame({"ic_id": s, "mfid": f"M{s}", "date": dates, "tn": tn, "tx": tx,
                                   "rr": rng.exponential(2, len(dates)), "n_obs": 48, "n_obs_rejetees": 0,
                                   "statut_extremes": "quotidien", "_commun": commun}))
    df = pd.concat(parts, ignore_index=True).drop(columns="_commun")

    def set_(s, d, **kv):
        m = (df.ic_id == s) & (df.date == pd.Timestamp(d))
        for k, v in kv.items():
            df.loc[m, k] = v

    # Canicule réelle : +12 °C sur TOUTES les stations pendant 5 jours -> ne doit pas être rejetée
    m = df.date.between("2021-07-10", "2021-07-14")
    df.loc[m, ["tn", "tx"]] += 12
    set_("A", "2021-05-27", tx=40.0)              # pic isolé de capteur sur une station -> flag_spatial
    set_("B", "2021-03-01", tx=60.0)              # hors bornes physiques
    set_("C", "2021-03-02", tn=15.0, tx=5.0)      # tn > tx
    set_("D", "2021-03-03", tx=np.nan)            # valeur manquante
    set_("E", "2021-03-04", rr=-3.0)              # pluie négative : n'invalide que rr
    set_("F", "2021-03-05", statut_extremes="partiel", n_obs=10)  # journée partielle : informatif
    df = df[~((df.ic_id == "F") & (df.date == pd.Timestamp("2021-03-06")))]  # jour absent de l'API
    return bs.appliquer_regles(bs.complete_calendar(df))


def ligne(df, s, d):
    return df[(df.ic_id == s) & (df.date == pd.Timestamp(d))].iloc[0]


def test_calendrier_complet(jeu):
    for _, g in jeu.groupby("ic_id"):
        assert len(g) == (g.date.max() - g.date.min()).days + 1
    assert ligne(jeu, "F", "2021-03-06").flag_absent


def test_pic_isole_rejete(jeu):
    r = ligne(jeu, "A", "2021-05-27")
    assert r.flag_spatial and not r.valide and np.isnan(r.tx) and r.tx_brut == 40.0


def test_canicule_generalisee_conservee(jeu):
    m = jeu.date.between("2021-07-10", "2021-07-14")
    assert jeu.loc[m, "valide"].all() and not jeu.loc[m, "flag_spatial"].any()


def test_hors_bornes_et_incoherence(jeu):
    assert ligne(jeu, "B", "2021-03-01").flag_hors_bornes
    assert ligne(jeu, "C", "2021-03-02").flag_incoherent
    assert not ligne(jeu, "B", "2021-03-01").valide and not ligne(jeu, "C", "2021-03-02").valide


def test_valeur_manquante(jeu):
    r = ligne(jeu, "D", "2021-03-03")
    assert r.flag_absent and not r.valide


def test_pluie_invalide_ne_touche_pas_les_temperatures(jeu):
    r = ligne(jeu, "E", "2021-03-04")
    assert r.flag_rr_invalide and np.isnan(r.rr) and r.valide and not np.isnan(r.tx)


def test_journee_partielle_informative(jeu):
    r = ligne(jeu, "F", "2021-03-05")
    assert r.flag_partiel and r.valide


def test_peu_de_faux_positifs(jeu):
    # hors des 6 cas injectés, la quasi-totalité des jours reste valide
    assert (~jeu.valide).sum() <= 6 + 3


def test_valeurs_brutes_conservees(jeu):
    assert jeu.tx_brut.notna().sum() >= jeu.tx.notna().sum()
    assert ((jeu.tn <= jeu.tx) | jeu.tn.isna() | jeu.tx.isna()).all()
