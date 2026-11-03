"""
Batch log — inventaire et suivi des batchs d'ingestion (S2 p15).

Un *batch* est l'unité d'ingestion, de replay et de suivi. Le log est stocké à part des données,
dans data/_meta/batchs.parquet, et garde une ligne par batch avec son cycle de vie :

    RECU  →  VERIFIE  →  CHARGE (Bronze écrit)  →  PUBLIE (Silver écrit)
    (en échec → corrigé puis relancé : le batch repart de VERIFIE)

Il porte l'idempotence : `deja_charge(batch_id, checksum)` permet d'ignorer un batch déjà traité
dont le contenu n'a pas changé (même checksum). Pur Python + pandas, aucune dépendance à Spark.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
META = ROOT / "data" / "_meta"
LOG = META / "batchs.parquet"

# Colonnes du log ; l'ordre fait foi pour l'affichage.
COLONNES = [
    "batch_id", "source", "kind", "ic_id", "run_date", "uri",
    "nb_fichiers", "nb_lignes", "checksum", "statut", "maj_le",
]
STATUTS = ["RECU", "VERIFIE", "CHARGE", "PUBLIE", "ECHEC"]
# Un batch est considéré « déjà traité » si son contenu est en place à l'un de ces statuts.
STATUTS_EN_PLACE = {"VERIFIE", "CHARGE", "PUBLIE"}


def _maintenant() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


class BatchLog:
    """Accès concurrent-naïf : lecture/écriture complète du parquet à chaque opération.
    Suffisant pour un pipeline par batchs ; un vrai socle utiliserait une table transactionnelle."""

    def __init__(self, chemin: Path = LOG):
        self.chemin = Path(chemin)
        self.chemin.parent.mkdir(parents=True, exist_ok=True)

    def lire(self) -> pd.DataFrame:
        if self.chemin.exists():
            return pd.read_parquet(self.chemin)
        return pd.DataFrame(columns=COLONNES)

    def _ecrire(self, df: pd.DataFrame) -> None:
        df = df[COLONNES].sort_values(["source", "kind", "batch_id"]).reset_index(drop=True)
        tmp = self.chemin.with_suffix(".parquet.tmp")
        df.to_parquet(tmp, index=False)    # écrire à côté puis publier : commit ~atomique
        tmp.replace(self.chemin)

    def deja_charge(self, batch_id: str, checksum: str) -> bool:
        """Vrai si le batch est déjà en place avec exactement ce checksum (→ ingestion idempotente)."""
        df = self.lire()
        m = df[(df.batch_id == batch_id) & (df.checksum == checksum) & (df.statut.isin(STATUTS_EN_PLACE))]
        return len(m) > 0

    def statut_de(self, batch_id: str) -> str | None:
        df = self.lire()
        m = df[df.batch_id == batch_id]
        return None if m.empty else m.iloc[-1]["statut"]

    def marquer(self, batch_id: str, statut: str, **champs) -> None:
        """Insère ou met à jour la ligne du batch (upsert par batch_id)."""
        assert statut in STATUTS, f"statut inconnu : {statut}"
        df = self.lire()
        ligne = {c: None for c in COLONNES}
        prec = df[df.batch_id == batch_id]
        if not prec.empty:                              # conserver les champs déjà connus
            ligne.update(prec.iloc[-1].to_dict())
        ligne.update(batch_id=batch_id, statut=statut, maj_le=_maintenant(), **champs)
        nouvelle = pd.DataFrame([ligne], columns=COLONNES)
        df = df[df.batch_id != batch_id]
        df = nouvelle if df.empty else pd.concat([df, nouvelle], ignore_index=True)
        self._ecrire(df)

    def reconciliation(self) -> pd.DataFrame:
        """Vue de synthèse pour le rapport : nb de batchs par statut."""
        df = self.lire()
        if df.empty:
            return df
        return df.groupby(["source", "kind", "statut"]).agg(
            batchs=("batch_id", "size"), lignes=("nb_lignes", "sum")).reset_index()


if __name__ == "__main__":
    log = BatchLog()
    df = log.lire()
    if df.empty:
        print("Batch log vide. Lancer src/promote_to_raw.py pour peupler la zone brute.")
    else:
        print(f"{len(df)} batch(s) dans {LOG.relative_to(ROOT)} :\n")
        print(df.to_string(index=False))
        print("\nRéconciliation :\n")
        print(log.reconciliation().to_string(index=False))
