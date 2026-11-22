"""
Tâche planifiée : calcul quotidien des prédictions.
Tournera sur le VPS (pas sur GitHub Actions).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)


async def _tache_quotidienne():
    """Exécutée chaque nuit à 03h00 (heure France)."""
    from app.database import AsyncSessionLocal
    from predictif.runner import run_pipeline

    logger.info("Début calcul prédictif — %s", datetime.now().isoformat())
    async with AsyncSessionLocal() as db:
        try:
            result = await run_pipeline(db_session=db)
            await db.commit()
            logger.info("Calcul prédictif terminé : %s", result)
        except Exception as e:
            await db.rollback()
            logger.error("Erreur calcul prédictif : %s", e, exc_info=True)
            raise


def demarrer_scheduler() -> AsyncIOScheduler:
    """Démarre le scheduler APScheduler. Appelé au démarrage de l'application FastAPI."""
    scheduler = AsyncIOScheduler(timezone="Europe/Paris")
    scheduler.add_job(
        _tache_quotidienne,
        CronTrigger(hour=3, minute=0),
        id="calcul_predictif_quotidien",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    logger.info("Scheduler démarré — tâche prédictive à 03h00 Europe/Paris")
    return scheduler
