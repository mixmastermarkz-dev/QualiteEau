"""
Source Hub'Eau : hydrométrie (débits) et piézométrie (nappes).

Détecte les réponses HTML de maintenance (voir shared.py du site principal).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date

import httpx
import pandas as pd

from predictif.sources.base import DataSource, SourceResult

logger = logging.getLogger(__name__)

HUBEAU_BASE = "https://hubeau.eaufrance.fr/api"
_TIMEOUT = httpx.Timeout(90.0)
_HEADERS = {"User-Agent": "mon-environnement.fr/predictif-1.0 (contact@mon-environnement.fr)"}


def _is_html_response(response: httpx.Response) -> bool:
    """Détecte une réponse HTML renvoyée lors d'une maintenance Hub'Eau."""
    ct = response.headers.get("content-type", "")
    if "text/html" in ct:
        return True
    if response.status_code == 200:
        try:
            text = response.text[:200]
            return text.strip().startswith("<!") or "<html" in text.lower()
        except Exception:
            return False
    return False


async def _get_pages(client: httpx.AsyncClient, url: str, params: dict) -> list[dict]:
    """Suit la pagination Hub'Eau (champ 'next'). Retourne [] si indisponible."""
    out = []
    next_url: str | None = url
    while next_url:
        try:
            r = await client.get(next_url, params=params if next_url == url else None, timeout=_TIMEOUT)
        except httpx.RequestError as e:
            logger.warning("Hub'Eau réseau : %s", e)
            break
        if _is_html_response(r):
            logger.warning("Hub'Eau retourne du HTML (maintenance probable) : %s", next_url[:80])
            break
        if r.status_code not in (200, 206):
            logger.warning("Hub'Eau HTTP %s : %s", r.status_code, next_url[:80])
            break
        data = r.json()
        out.extend(data.get("data", []))
        next_url = data.get("next")
        params = {}  # déjà dans l'URL pour les pages suivantes
    return out


class HubeauHydroSource(DataSource):
    """Débits moyens journaliers (QmnJ) via Hub'Eau hydrométrie v2."""

    @property
    def name(self) -> str:
        return "hubeau_hydro"

    async def fetch(self, depts: list[str], date_debut: str, date_fin: str) -> SourceResult:
        all_rows = []
        async with httpx.AsyncClient(headers=_HEADERS) as client:
            for dept in depts:
                # 1. Stations du département
                stations_data = await _get_pages(client, f"{HUBEAU_BASE}/v2/hydrometrie/referentiel/stations",
                                                  {"code_departement": dept, "en_service": "true", "size": 1000})
                if not stations_data:
                    continue
                codes = [s["code_station"] for s in stations_data if s.get("code_station")][:50]  # limite

                # 2. Observations par station (par lots pour éviter les timeouts)
                for code in codes:
                    obs = await _get_pages(client, f"{HUBEAU_BASE}/v2/hydrometrie/obs_elab", {
                        "code_entite": code,
                        "grandeur_hydro_elab": "QmnJ",
                        "date_debut_obs_elab": date_debut,
                        "date_fin_obs_elab": date_fin,
                        "size": 20000,
                        "fields": "date_obs_elab,resultat_obs_elab",
                    })
                    for o in obs:
                        all_rows.append({"code_station": code, "departement": dept,
                                         "date": o.get("date_obs_elab"), "valeur": o.get("resultat_obs_elab")})

        if not all_rows:
            return SourceResult(data=None, available=False, error="Aucune donnée hydrométrique", source_name=self.name)

        df = pd.DataFrame(all_rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None).dt.normalize()
        df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce")
        return SourceResult(data=df.dropna(subset=["date", "valeur"]), available=True, source_name=self.name)


class HubeauPiezoSource(DataSource):
    """Niveaux des nappes phréatiques via Hub'Eau piézométrie v1."""

    @property
    def name(self) -> str:
        return "hubeau_piezo"

    async def fetch(self, depts: list[str], date_debut: str, date_fin: str) -> SourceResult:
        all_rows = []
        async with httpx.AsyncClient(headers=_HEADERS) as client:
            for dept in depts:
                stations_data = await _get_pages(client, f"{HUBEAU_BASE}/v1/niveaux_nappes/stations",
                                                  {"code_departement": dept, "size": 5000})
                if not stations_data:
                    continue

                df_st = pd.DataFrame(stations_data)
                fin_col = df_st.get("date_fin_mesure") if "date_fin_mesure" in df_st.columns else None
                deb_col = df_st.get("date_debut_mesure") if "date_debut_mesure" in df_st.columns else None
                if fin_col is not None and deb_col is not None:
                    fin_dt = pd.to_datetime(fin_col, errors="coerce")
                    deb_dt = pd.to_datetime(deb_col, errors="coerce")
                    ok = (fin_dt > pd.Timestamp.today() - pd.Timedelta(days=90)) & \
                         (deb_dt < pd.Timestamp("2016-01-01"))
                    codes = df_st.loc[ok, "code_bss"].dropna().unique().tolist()
                else:
                    codes = df_st["code_bss"].dropna().unique().tolist()[:30]

                for code in codes[:40]:
                    obs = await _get_pages(client, f"{HUBEAU_BASE}/v1/niveaux_nappes/chroniques", {
                        "code_bss": code,
                        "date_debut_mesure": date_debut,
                        "date_fin_mesure": date_fin,
                        "size": 20000,
                        "fields": "date_mesure,niveau_nappe_eau",
                    })
                    for o in obs:
                        all_rows.append({"code_bss": code, "departement": dept,
                                         "date": o.get("date_mesure"), "valeur": o.get("niveau_nappe_eau")})

        if not all_rows:
            return SourceResult(data=None, available=False, error="Aucune donnée piézométrique", source_name=self.name)

        df = pd.DataFrame(all_rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None).dt.normalize()
        df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce")
        return SourceResult(data=df.dropna(subset=["date", "valeur"]), available=True, source_name=self.name)
