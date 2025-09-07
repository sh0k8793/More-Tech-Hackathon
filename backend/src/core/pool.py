from core.settings import settings
from psycopg_pool import AsyncConnectionPool
from utils.logger import logger

pool = AsyncConnectionPool(
    f"host={settings.DB_HOSTNAME} dbname={settings.DB_NAME} user={settings.DB_USERNAME} password={settings.DB_PASSWORD}",
    open=False,
    min_size=1
)

async def get_conn():
    if pool.closed:
        logger.warning("Pool is closed. Opening it...")
        await pool.open()
        await pool.wait()

    async with pool.connection() as conn:
        yield conn
