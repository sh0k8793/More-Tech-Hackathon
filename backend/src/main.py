from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.v1.router import v1
from core.pool import pool
import logging


# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(asctime)s - %(message)s",
    force=True
)
logger = logging.getLogger(__name__)


app = FastAPI()


# opening pools
@app.on_event("startup")
async def startup_event():
    logger.info("Application starting up...")
    global stats_cache
    await pool.open()
    await pool.wait()
    logger.info("Pool opened")
    

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutting down...")
    try:
        logger.info("Gracefully stopping...")
        logger.info("Closing pool...")
        await pool.close()
    except Exception as e:
        logger.error(f"{e}")


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
