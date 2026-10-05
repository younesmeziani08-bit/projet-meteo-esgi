"""Règles « custom » du contrat Gold : absence de fuite d'information du futur."""
import numpy as np
import pandas as pd
import pytest

import build_gold as bg
from conftest import SILVER_PQ, besoin

pytestmark = [pytest.mark.donnees, besoin(SILVER_PQ)]


@pytest.fixture(scope="module")
def base():
    s = bg.load_silver()
    clim = bg.climatologie_train(s)
    return s, clim, bg.build_features(s, clim)


def test_variables_independantes_du_futur(base):
    s, clim, full = base
    T = pd.Timestamp("2024-06-30")
    tronque = bg.build_features(s[s.date <= T], clim)
    d = bg.dictionnaire(list(full.columns))
    feats = d[d.role == "variable"].colonne.tolist()
    a = full[full.date <= T].set_index(["ic_id", "date"])[feats].sort_index()
    b = tronque.set_index(["ic_id", "date"])[feats].sort_index()
    diff = ((a - b).abs() > 1e-9) & ~(a.isna() & b.isna())
    assert int(diff.values.sum()) == 0, diff.sum()[diff.sum() > 0].to_dict()


@pytest.mark.parametrize("v", ["tn", "tx"])
@pytest.mark.parametrize("h", [1, 4, 7])
def test_cibles_exactes(base, v, h):
    s, _, full = base
    smp = full.sample(3000, random_state=h)
    ref = s.set_index(["ic_id", "date"])[v].reindex(list(zip(smp.ic_id.astype(str), smp.date + pd.Timedelta(days=h))))
    assert np.allclose(ref.to_numpy(), smp[f"cible_{v}_h{h}"].to_numpy(), equal_nan=True)


@pytest.mark.parametrize("split,fin", [("train", bg.FIN_TRAIN), ("valid", bg.FIN_VALID)])
def test_cibles_ne_debordent_pas(base, split, fin):
    *_, full = base
    assert full[full.split == split].date.max() + pd.Timedelta(days=max(bg.HORIZONS)) <= fin


def test_climatologie_apprise_sur_train_seul(base):
    s, clim, _ = base
    c2 = bg.climatologie_train(s[s.date <= bg.FIN_TRAIN])
    a, b = (c.sort_values(["ic_id", "doy"]).reset_index(drop=True) for c in (clim, c2))
    pd.testing.assert_frame_equal(a, b)


def test_baselines(base):
    *_, full = base
    pd.testing.assert_series_equal(full.base_persistance_tx_h3, full.tx, check_names=False)
    assert full.loc[full.split == "test", "base_clim_tx_h1"].notna().all()
