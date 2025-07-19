from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.logger import Logger
from cricket_etl.pipeline import ingest_flow, raw_flow, bronze_flow, silver_flow

logger = Logger("main")

def main(catalog: Catalog):
    ingest_flow(catalog)
    logger.info("Ingest completed")
    raw_flow(catalog)
    logger.info("Raw completed")
    bronze_flow(catalog)
    logger.info("Bronze completed")
    silver_flow(catalog)
    logger.info("Silver completed")

    logger.info("Main pipeline completed")


if __name__ == "__main__":
    catalog = Catalog()
    main(catalog)