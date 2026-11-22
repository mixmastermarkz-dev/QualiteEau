"""
Configuration centralisée — chargée depuis les variables d'environnement.

Règle de sécurité : toute variable sans valeur par défaut est OBLIGATOIRE.
L'application refuse de démarrer si une variable manque.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="backend/.env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Base de données — obligatoire
    database_url: str

    # Sécurité — obligatoires
    session_secret: str
    magic_link_secret: str

    # Environnement
    environment: str = "development"

    # CORS
    frontend_origin: str = "http://localhost:3000"

    # Email — obligatoires sauf en développement
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    email_from: str = ""

    # Stripe — obligatoires pour le paiement
    stripe_secret_key: str = ""
    stripe_webhook_secret: str = ""
    stripe_price_a: str = ""
    stripe_price_b: str = ""
    stripe_price_c: str = ""

    # Sessions
    session_duration_days: int = 30         # Expiration absolue
    session_inactivity_days: int = 7        # Expiration par inactivité

    # Lien magique
    magic_link_ttl_minutes: int = 15        # Durée de validité du lien

    # Paiement
    payment_grace_period_days: int = 7     # Délai après échec de paiement avant suspension

    @property
    def session_cookie_name(self) -> str:
        """__Host- en production (HTTPS requis), 'session' en dev."""
        return "__Host-session" if self.is_production else "session"

    def get_price_id_for_plan(self, plan_value: str) -> str | None:
        """Retourne le price_id Stripe pour un nom de formule (A/B/C). None si inconnu."""
        mapping = {"A": self.stripe_price_a, "B": self.stripe_price_b, "C": self.stripe_price_c}
        return mapping.get(plan_value.upper()) or None

    @property
    def price_plan_map(self) -> dict:
        """Correspondance price_id Stripe → Plan. Source unique de vérité côté serveur."""
        from app.models.subscription import Plan
        result: dict[str, Plan] = {}
        if self.stripe_price_a:
            result[self.stripe_price_a] = Plan.A
        if self.stripe_price_b:
            result[self.stripe_price_b] = Plan.B
        if self.stripe_price_c:
            result[self.stripe_price_c] = Plan.C
        return result

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    def assert_production_secrets(self) -> None:
        """Appelé au démarrage en production : plante si un secret obligatoire manque."""
        required = {
            "smtp_host": self.smtp_host,
            "smtp_user": self.smtp_user,
            "smtp_password": self.smtp_password,
            "stripe_secret_key": self.stripe_secret_key,
            "stripe_webhook_secret": self.stripe_webhook_secret,
            "stripe_price_a": self.stripe_price_a,
            "stripe_price_b": self.stripe_price_b,
            "stripe_price_c": self.stripe_price_c,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(
                f"Variables d'environnement obligatoires manquantes en production : "
                f"{', '.join(missing)}"
            )


settings = Settings()
