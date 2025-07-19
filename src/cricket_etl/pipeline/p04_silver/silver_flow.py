import duckdb as db

from cricket_etl.helpers.catalog import Catalog
from cricket_etl.pipeline.p04_silver.create_schema import create_schema
from cricket_etl.pipeline.p04_silver.create_views import create_views
from cricket_etl.pipeline.p04_silver.create_wide_table import create_wide_table, write_wide_table_parquet
from cricket_etl.helpers.logger import Logger

logger = Logger("silver")


def silver_flow(catalog: Catalog):
    create_schema(catalog)
    logger.info("Create schema completed")
    create_views(catalog)
    logger.info("Create views completed")
    create_wide_table(catalog)
    write_wide_table_parquet(catalog)
    logger.info("Create wide table completed")


if __name__ == "__main__":
    catalog = Catalog()
    silver_flow(catalog)