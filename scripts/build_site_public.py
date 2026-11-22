"""
Construit le dossier de publication en copiant uniquement les fichiers publics
depuis la racine du dépôt vers build/site/ (liste blanche).

Appelé avant de démarrer le conteneur Docker pour s'assurer que le volume
./build/site monté dans le conteneur ne contient jamais de fichiers sensibles.

Usage :
    python scripts/build_site_public.py
    python scripts/build_site_public.py --root /chemin/repo --out build/site
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

# Fichiers individuels autorisés à la racine du dépôt
FICHIERS_AUTORISES: list[str] = [
    "index.html",
    "contact.html",
    "mentions-legales.html",
    "politique-confidentialite.html",
    "sources-et-methodologie.html",
    "robots.txt",
    "sitemap.xml",
    "CNAME",
]

# Dossiers entiers autorisés
DOSSIERS_AUTORISES: list[str] = [
    "eau-potable",
    "departement",
    "tarifs",
    "assets",
]


def build(repo_root: Path, out_dir: Path) -> int:
    """Construit le dossier de publication. Retourne le nombre de fichiers copiés."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    copied = 0

    for name in FICHIERS_AUTORISES:
        src = repo_root / name
        if src.is_file():
            shutil.copy2(src, out_dir / name)
            copied += 1

    for name in DOSSIERS_AUTORISES:
        src = repo_root / name
        if src.is_dir():
            shutil.copytree(src, out_dir / name)
            copied += sum(1 for _ in (out_dir / name).rglob("*") if _.is_file())

    return copied


def main() -> None:
    parser = argparse.ArgumentParser(description="Construit le dossier de publication (liste blanche).")
    parser.add_argument("--root", default=".", help="Racine du dépôt (défaut : répertoire courant)")
    parser.add_argument("--out", default="build/site", help="Dossier de sortie (défaut : build/site)")
    args = parser.parse_args()

    repo_root = Path(args.root).resolve()
    out_dir = Path(args.out).resolve()

    n = build(repo_root, out_dir)
    print(f"Site public construit dans {out_dir} ({n} fichiers).")


if __name__ == "__main__":
    main()
