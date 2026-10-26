#!/usr/bin/env python3
"""
Simulateur de flux Vélib' pour Structured Streaming.

Rejoue les données historiques Parquet en écrivant des fichiers JSON
dans un répertoire surveillé par Spark, à un rythme configurable.

Usage
-----
    python simulateur_flux.py --input data/output/disponibilite_consolidee.parquet
                               --output data/output/stream_input
                               --vitesse 3
                               --batch-size 50
                               --intervalle 5

Options
-------
--input      : Répertoire Parquet source (produit au Jour 1).
--output     : Répertoire de sortie surveillé par Spark readStream.
--vitesse    : Facteur d'accélération du temps (1 = temps réel, 3 = x3 plus rapide).
--batch-size : Nombre de snapshots par fichier JSON émis.
--intervalle : Intervalle entre deux émissions, en secondes réelles.
--annee      : Année à rejouer (défaut : 2022).
--mois       : Mois de départ (défaut : 1).
"""

import argparse
import json
import time
import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulateur de flux Vélib' pour Structured Streaming."
    )
    parser.add_argument("--input",
        default="data/output/disponibilite_consolidee.parquet",
        help="Répertoire Parquet source.")
    parser.add_argument("--output",
        default="data/output/stream_input",
        help="Répertoire de sortie surveillé par Spark.")
    parser.add_argument("--vitesse", type=float, default=3.0,
        help="Facteur d'accélération du temps (défaut : 3).")
    parser.add_argument("--batch-size", type=int, default=50,
        help="Nombre de lignes par fichier JSON (défaut : 50).")
    parser.add_argument("--intervalle", type=float, default=5.0,
        help="Secondes réelles entre deux émissions (défaut : 5).")
    parser.add_argument("--annee", type=int, default=2022,
        help="Année à rejouer (défaut : 2022).")
    parser.add_argument("--mois", type=int, default=1,
        help="Mois de départ (défaut : 1).")
    return parser.parse_args()


def charger_partition(chemin: str, annee: int, mois: int) -> pd.DataFrame:
    """Charge une partition Parquet pour un mois donné.

    Args:
        chemin : Chemin vers le répertoire Parquet partitionné.
        annee  : Année de la partition.
        mois   : Mois de la partition.

    Returns:
        DataFrame Pandas trié par horodatage, ou DataFrame vide si absent.
    """
    partition = Path(chemin) / f"annee={annee}" / f"mois={mois:02d}"
    if not partition.exists():
        # Essai sans zéro de remplissage
        partition = Path(chemin) / f"annee={annee}" / f"mois={mois}"
    if not partition.exists():
        print(f"  [INFO] Partition manquante : annee={annee}/mois={mois}", file=sys.stderr)
        return pd.DataFrame()

    try:
        df = pd.read_parquet(partition, columns=[
            "nom_station", "capacite",
            "velos_meca", "velos_elec", "velos_total", "horodatage"
        ])
        df["horodatage"] = pd.to_datetime(df["horodatage"], utc=True)
        return df.sort_values("horodatage").reset_index(drop=True)
    except Exception as e:
        print(f"  [ERREUR] Lecture de {partition} : {e}", file=sys.stderr)
        return pd.DataFrame()


def serialiser_ligne(row: dict) -> dict:
    """Convertit une ligne Pandas en dictionnaire JSON-sérialisable.

    Args:
        row : Dictionnaire issu d'un enregistrement Pandas.

    Returns:
        Dictionnaire avec le timestamp converti en ISO 8601.
    """
    d = dict(row)
    ts = d.get("horodatage")
    if hasattr(ts, "isoformat"):
        d["horodatage"] = ts.isoformat()
    elif hasattr(ts, "item"):
        # numpy datetime64 -> Python datetime
        d["horodatage"] = pd.Timestamp(ts).isoformat()
    # Cast des entiers numpy -> int Python pour json.dumps
    for k, v in d.items():
        if hasattr(v, "item"):
            d[k] = v.item()
    return d


def main():
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Simulateur de flux Vélib' démarré")
    print(f"  Source   : {args.input}")
    print(f"  Sortie   : {args.output}")
    print(f"  Vitesse  : x{args.vitesse}")
    print(f"  Batch    : {args.batch_size} lignes / fichier")
    print(f"  Intervalle : {args.intervalle} s (réelles)")
    print(f"  Départ   : {args.annee}-{args.mois:02d}")
    print("  Ctrl+C pour arrêter.\n")

    annee, mois = args.annee, args.mois
    fichier_index = 0
    lignes_emises = 0

    while True:
        df_mois = charger_partition(args.input, annee, mois)

        if df_mois.empty:
            # Passer au mois suivant
            mois += 1
            if mois > 12:
                mois = 1
                annee += 1
                if annee > args.annee + 1:
                    # Rebouclage après 2 ans
                    annee = args.annee
                    mois  = args.mois
                    print("  [INFO] Rebouclage au début.", file=sys.stderr)
            continue

        print(f"  Chargement : {annee}-{mois:02d}  ({len(df_mois):,} lignes)")

        # Découpe du mois en mini-batchs
        for i in range(0, len(df_mois), args.batch_size):
            chunk = df_mois.iloc[i : i + args.batch_size]
            if chunk.empty:
                continue

            # Horodatage "temps simulé" -> horodatage "temps réel maintenant"
            # On recale les timestamps pour qu'ils ressemblent à du live
            maintenant = datetime.now(timezone.utc)
            ecart = maintenant - chunk["horodatage"].iloc[0].to_pydatetime()

            payload = []
            for _, row in chunk.iterrows():
                d = serialiser_ligne(row.to_dict())
                # Recalage du timestamp
                ts_recale = (
                    row["horodatage"].to_pydatetime() + ecart
                )
                d["horodatage"] = ts_recale.strftime("%Y-%m-%dT%H:%M:%SZ")
                payload.append(d)

            # Écriture du fichier JSON atomique
            nom_fichier = output_dir / f"velib_{fichier_index:06d}.json"
            nom_tmp     = output_dir / f".tmp_{fichier_index:06d}.json"

            with open(nom_tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            # Renommage atomique : Spark ne lira le fichier que lorsqu'il est complet
            nom_tmp.rename(nom_fichier)

            fichier_index += 1
            lignes_emises += len(payload)

            ts_str = payload[0]["horodatage"]
            print(
                f"  [{fichier_index:04d}] {nom_fichier.name}"
                f"  {len(payload)} lignes"
                f"  (ts={ts_str})"
                f"  total={lignes_emises:,}"
            )

            time.sleep(args.intervalle)

        # Passage au mois suivant
        mois += 1
        if mois > 12:
            mois = 1
            annee += 1


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSimulateur arrêté proprement.")
        sys.exit(0)
