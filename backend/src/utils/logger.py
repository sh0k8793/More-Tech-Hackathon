# logging
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(asctime)s - %(message)s",
    force=True
)
logger = logging.getLogger(__name__)
