from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    OLLAMA_API_BASE: str
    OLLAMA_API_KEY: str
    OLLAMA_MODEL: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings() #ty: ignore
