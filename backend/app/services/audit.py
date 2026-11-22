"""
Service de journalisation d'audit.

Règles :
- Append-only : aucun UPDATE ni DELETE sur audit_logs.
- Pas de donnée personnelle ni de secret dans les champs.
- L'IP est hashée (SHA-256) pour détecter les anomalies sans stocker la valeur brute.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit_log import AuditLog

logger = logging.getLogger(__name__)


def _hash_ip(ip: str | None) -> str | None:
    if not ip:
        return None
    return hashlib.sha256(ip.encode()).hexdigest()


async def log_action(
    db: AsyncSession,
    action: str,
    *,
    actor_user_id: str | None = None,
    org_id: str | None = None,
    target_type: str | None = None,
    target_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    ip_address: str | None = None,
) -> None:
    """
    Enregistre une action dans le journal d'audit.

    `metadata` : informations contextuelles libres — sans PII ni secret.
    """
    entry = AuditLog(
        actor_user_id=actor_user_id,
        org_id=org_id,
        action=action,
        target_type=target_type,
        target_id=target_id,
        metadata_json=json.dumps(metadata, ensure_ascii=False) if metadata else None,
        ip_hash=_hash_ip(ip_address),
        created_at=datetime.now(timezone.utc),
    )
    db.add(entry)
    # Pas de flush immédiat — le commit de la transaction englobante suffit.
    logger.info("audit | action=%s actor=%s org=%s target=%s/%s",
                action, actor_user_id or "system", org_id, target_type, target_id)
