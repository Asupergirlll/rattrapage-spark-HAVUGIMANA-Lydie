#!/usr/bin/env python3
"""
Nettoie les kernels Jupyter "fantômes" créés par VSCode.

Contexte
--------
Quand VSCode se (re)connecte à un serveur Jupyter distant (docker-compose de
ce projet), il crée parfois un kernel "fantôme" au lieu de réutiliser celui
du vrai fichier notebook -- reconnaissable à un chemin de session du type
"NomDuNotebook-jvsc-....ipynb" au lieu de "notebooks/NomDuNotebook.ipynb".

Chaque fantôme peut démarrer sa propre SparkSession (4 Go de heap JVM
chacune) -- ils s'accumulent au fil des reconnexions et finissent par
épuiser la mémoire du conteneur (OOM, JAVA_GATEWAY_EXITED...).

Ce script ferme uniquement les sessions "-jvsc-" ; il ne touche jamais aux
sessions attachées à un vrai fichier de notebooks/.

Usage
-----
    python scripts/nettoyer_kernels.py
    python scripts/nettoyer_kernels.py --token climacity --port 8888
"""

import argparse
import sys
import urllib.request
import json


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8888)
    parser.add_argument("--token", default="climacity")
    return parser.parse_args()


def api(base_url: str, token: str, path: str, method: str = "GET"):
    req = urllib.request.Request(
        f"{base_url}{path}",
        method=method,
        headers={"Authorization": f"token {token}"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status == 204 or resp.length == 0:
            return None
        return json.loads(resp.read())


def main():
    args = parse_args()
    base_url = f"http://{args.host}:{args.port}"

    try:
        sessions = api(base_url, args.token, "/api/sessions")
    except Exception as e:
        print(f"Impossible de contacter le serveur Jupyter ({base_url}) : {e}", file=sys.stderr)
        sys.exit(1)

    print(f"{len(sessions)} session(s) trouvée(s) sur {base_url}\n")

    fantomes = [s for s in sessions if "-jvsc-" in s["path"]]
    reelles  = [s for s in sessions if "-jvsc-" not in s["path"]]

    for s in reelles:
        print(f"  [GARDÉE]  {s['path']}  ({s['kernel']['execution_state']})")

    if not fantomes:
        print("\nAucun kernel fantôme à nettoyer.")
        return

    print()
    for s in fantomes:
        print(f"  [FERMÉE]  {s['path']}  ({s['kernel']['execution_state']})")
        api(base_url, args.token, f"/api/sessions/{s['id']}", method="DELETE")

    print(f"\n{len(fantomes)} kernel(s) fantôme(s) fermé(s).")


if __name__ == "__main__":
    main()
