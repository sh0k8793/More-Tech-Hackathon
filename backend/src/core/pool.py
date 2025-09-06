from core.settings import DB_NAME, DB_USERNAME, DB_PASSWORD, DB_HOSTNAME
from psycopg_pool import AsyncConnectionPool
import logging

logger = logging.getLogger(__name__)

pool = AsyncConnectionPool(
    f"host={DB_HOSTNAME} dbname={DB_NAME} user={DB_USERNAME} password={DB_PASSWORD}",
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
