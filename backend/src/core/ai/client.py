from openai import OpenAI
from core.ai import settings

def get_client():
    return OpenAI(
        api_key=settings.OLLAMA_API_KEY,
        base_url=settings.OLLAMA_API_BASE,
    )

