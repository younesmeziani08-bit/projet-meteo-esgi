"""
Publication atomique et recovery (S3 p15, S1 p45).

`publier_atomique` écrit d'abord dans un dossier de staging, puis publie partition par partition par
un `os.replace` (atomique sur le même système de fichiers). Garanties :
  - aucune partition publiée à moitié (le replace est tout-ou-rien) ;
  - après une interruption, une relance ne refait QUE les partitions non encore publiées
    (« retry ciblé ») et ne crée aucun double comptage (idempotence par partition).

Limite documentée (S3 p15) : sur un object storage, le rename n'est pas toujours atomique ; log et
données sont mis à jour en deux temps. Ici (FS local) le replace de répertoire est atomique.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path


def partitions(dossier: Path) -> list[str]:
    """Noms des partitions (clé=valeur) présentes dans un dossier de table."""
    if not dossier.exists():
        return []
    return sorted(p.name for p in dossier.iterdir() if p.is_dir() and "=" in p.name)


def publier_atomique(df, dest: Path, partition_cols: list[str],
                     crasher_apres: int | None = None) -> list[str]:
    """Écrit df dans un staging puis publie ses partitions une à une dans `dest`.
    Les partitions déjà présentes dans `dest` sont considérées publiées et sautées (retry ciblé).
    `crasher_apres=k` lève une exception après k publications (interruption simulée)."""
    dest = Path(dest)
    staging = dest.with_name(dest.name + "__staging")
    if staging.exists():
        shutil.rmtree(staging)
    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(str(staging))   # écrire à part
    dest.mkdir(parents=True, exist_ok=True)

    a_publier = [p for p in partitions(staging) if not (dest / p).exists()]  # seules les manquantes
    publiees = []
    for i, nom in enumerate(a_publier):
        if crasher_apres is not None and i >= crasher_apres:
            raise RuntimeError(f"interruption simulée après {len(publiees)} partition(s) publiée(s)")
        os.replace(staging / nom, dest / nom)          # commit atomique de la partition
        publiees.append(nom)
    shutil.rmtree(staging, ignore_errors=True)
    return publiees
