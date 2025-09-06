import logging
from contextlib import asynccontextmanager

from api.v1.router import v1
from core.pool import pool
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

stats_cache = None
# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(asctime)s - %(message)s",
    force=True
)
logger = logging.getLogger(__name__)



# TODO: Refactor: move lifespan manager to lifespan.py
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application starting up...")
    global stats_cache
    await pool.open()
    await pool.wait()
    logger.info("Pool opened")

    yield
    print("Application shutdown initiated.")
    logger.info("Application shutting down...")
    try:
        logger.info("Gracefully stopping...")
        logger.info("Closing pool...")
        await pool.close()
    except Exception as e:
        logger.error(f"{e}")


app = FastAPI(lifespan=lifespan)

# including routers
app.include_router(v1)


# убрать при деплое
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
