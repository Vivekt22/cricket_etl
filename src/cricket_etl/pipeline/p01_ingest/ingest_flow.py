from cricket_etl.helpers.catalog import Catalog
from cricket_etl.pipeline.p01_ingest.ingest import ingest
from cricket_etl.helpers.logger import Logger

logger = Logger("ingest")

def ingest_flow(catalog: Catalog):
    ingest(catalog)
    logger.info("Ingest flow completed")