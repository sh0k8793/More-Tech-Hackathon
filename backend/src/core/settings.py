from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    DB_NAME: str
    DB_USERNAME: str
    DB_PASSWORD: str
    DB_HOSTNAME: str = "postgres"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings() #ty: ignore
