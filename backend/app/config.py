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
