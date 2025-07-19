from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.logger import Logger
from cricket_etl.pipeline import ingest_flow, raw_flow, bronze_flow, silver_flow, model_flow
from cricket_etl.helpers.duckdb_utils import DuckDBUtils

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
    model_flow(catalog)
    logger.info("Model completed")
    DuckDBUtils.vacuum_database(catalog.database)
    logger.info("Database vacuum completed")

    logger.info("Main pipeline completed")


if __name__ == "__main__":
    catalog = Catalog()
    main(catalog)