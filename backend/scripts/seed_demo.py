"""
Crée (ou réutilise) les 4 comptes de démonstration et affiche les liens magiques.

Usage depuis le répertoire backend/ :
    python -m scripts.seed_demo

Les 4 comptes :
    demo.gratuit@test.local   → Plan FREE
    demo.essentiel@test.local → Plan A  (Essentiel)
    demo.pro@test.local       → Plan B  (Pro)
    demo.ultimate@test.local  → Plan C  (Ultimate)
"""

from __future__ import annotations

import asyncio
import os
import sys

# S'assurer que les imports relatifs fonctionnent
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("TESTING", "1")

from app.database import AsyncSessionLocal
from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.services.auth import create_magic_token, get_or_create_user

DEMO_ACCOUNTS = [
    ("demo.gratuit@test.local",   Plan.FREE),
    ("demo.essentiel@test.local", Plan.A),
    ("demo.pro@test.local",       Plan.B),
    ("demo.ultimate@test.local",  Plan.C),
]

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


async def seed() -> None:
    from sqlalchemy import select

    async with AsyncSessionLocal() as db:
        print("\n" + "═" * 70)
        print("  COMPTES DE DÉMONSTRATION — mon-environnement.fr")
        print("═" * 70)

        for email, plan in DEMO_ACCOUNTS:
            # Crée ou récupère l'utilisateur
            user = await get_or_create_user(db, email)

            # Met à jour l'abonnement au plan voulu
            result = await db.execute(
                select(Subscription).where(Subscription.org_id == user.org_id)
            )
            sub = result.scalar_one_or_none()
            if sub is None:
                sub = Subscription(
                    org_id=user.org_id,
                    plan=plan,
                    status=SubscriptionStatus.ACTIVE,
                )
                db.add(sub)
            else:
                sub.plan = plan
                sub.status = SubscriptionStatus.ACTIVE

            # Génère un jeton magique (valide 15 min par défaut)
            raw_token = await create_magic_token(db, user)
            link = f"{BASE_URL}/auth/verify?token={raw_token}"

            plan_label = {
                Plan.FREE: "Gratuit  ",
                Plan.A:    "Essentiel",
                Plan.B:    "Pro      ",
                Plan.C:    "Ultimate ",
            }[plan]

            print(f"\n  [{plan_label}] {email}")
            print(f"  → {link}")

        await db.commit()

        print("\n" + "═" * 70)
        print("  Copiez un lien ci-dessus et ouvrez-le dans votre navigateur.")
        print("  Les liens expirent dans 15 minutes.")
        print("═" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(seed())
