import time
from cricket_etl.helpers.catalog import Catalog
from cricket_etl.pipeline.p01_ingest.ingest import ingest
from cricket_etl.helpers.logger import Logger

logger = Logger("ingest")

def ingest_flow(catalog: Catalog):
    start = time.time()
    try:
        ingest(catalog)
        logger.info("Ingest flow completed")
    except Exception as e:
        logger.error(f"Ingest flow failed: {e}")
        raise
    finally:
        logger.info(f"Ingest duration: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    catalog = Catalog()
    ingest_flow(catalog)