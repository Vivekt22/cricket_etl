import time
from cricket_etl.helpers.catalog import Catalog
from cricket_etl.pipeline.p02_raw.raw import convert_ingest_json_to_pkl
from cricket_etl.helpers.logger import Logger

logger = Logger("raw")

def raw_flow(catalog: Catalog):
    start = time.time()
    try:
        convert_ingest_json_to_pkl(catalog)
        logger.info("Raw flow completed")
    except Exception as e:
        logger.error(f"Raw flow failed: {e}")
        raise
    finally:
        logger.info(f"Raw duration: {time.time() - start:.2f} seconds")