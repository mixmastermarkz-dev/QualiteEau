"""
Service d'envoi d'e-mails.

En développement (SMTP_HOST vide) : affiche le lien dans les logs.
En production : envoi SMTP via aiosmtplib.

Aucun secret ni donnée personnelle n'est logué.
"""

from __future__ import annotations

import logging

from app.config import settings

logger = logging.getLogger(__name__)


async def send_magic_link(to_email: str, magic_link_url: str) -> None:
    """
    Envoie un e-mail contenant le lien magique de connexion.

    En dev, logue uniquement le lien (pas l'adresse e-mail complète).
    """
    if not settings.smtp_host:
        # Mode développement — lien visible dans les logs du container
        domain = to_email.split("@")[-1] if "@" in to_email else "?"
        logger.info(
            "[DEV] Lien magique pour *@%s → %s",
            domain,
            magic_link_url,
        )
        return

    await _send_smtp(
        to_email=to_email,
        subject="Votre lien de connexion — mon-environnement.fr",
        html_body=_magic_link_html(magic_link_url),
        text_body=_magic_link_text(magic_link_url),
    )


# ── Templates ─────────────────────────────────────────────────────────────────

def _magic_link_text(url: str) -> str:
    return (
        f"Bonjour,\n\n"
        f"Cliquez sur ce lien pour vous connecter à mon-environnement.fr :\n\n"
        f"{url}\n\n"
        f"Ce lien est valable {settings.magic_link_ttl_minutes} minutes et ne peut être utilisé qu'une seule fois.\n\n"
        f"Si vous n'avez pas demandé ce lien, ignorez cet e-mail.\n"
    )


def _magic_link_html(url: str) -> str:
    ttl = settings.magic_link_ttl_minutes
    return f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:24px">
  <p>Bonjour,</p>
  <p>Cliquez sur le bouton ci-dessous pour vous connecter à <strong>mon-environnement.fr</strong> :</p>
  <p style="text-align:center;margin:32px 0">
    <a href="{url}"
       style="background:#2563eb;color:#fff;text-decoration:none;padding:12px 24px;border-radius:6px;font-weight:bold">
      Se connecter
    </a>
  </p>
  <p style="color:#6b7280;font-size:14px">
    Ce lien est valable <strong>{ttl} minutes</strong> et ne peut être utilisé qu'une seule fois.<br>
    Si vous n'avez pas demandé ce lien, ignorez cet e-mail.
  </p>
  <hr style="border:none;border-top:1px solid #e5e7eb;margin:24px 0">
  <p style="color:#9ca3af;font-size:12px">
    mon-environnement.fr — Données environnementales en Occitanie
  </p>
</body>
</html>"""


# ── Envoi SMTP ────────────────────────────────────────────────────────────────

async def _send_smtp(
    to_email: str,
    subject: str,
    html_body: str,
    text_body: str,
) -> None:
    import aiosmtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = settings.email_from
    msg["To"] = to_email
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    await aiosmtplib.send(
        msg,
        hostname=settings.smtp_host,
        port=settings.smtp_port,
        username=settings.smtp_user or None,
        password=settings.smtp_password or None,
        start_tls=True,
    )
    logger.info("[EMAIL] Lien magique envoyé à *@%s", to_email.split("@")[-1])
