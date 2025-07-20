import time

from cricket_etl.helpers.catalog import Catalog
from cricket_etl.pipeline.p05_model.create_dim_tables import create_dim_tables
from cricket_etl.pipeline.p05_model.create_fact_tables import create_fact_cricket_table
from cricket_etl.pipeline.p05_model.write_model_parquets import write_model_parquets
from cricket_etl.helpers.logger import Logger

logger = Logger("model")


def model_flow(catalog: Catalog):
    start = time.time()
    try:
        create_dim_tables(catalog)
        create_fact_cricket_table(catalog)
        write_model_parquets(catalog)
    except Exception as e:
        logger.error(f"Model flow failed: {e}")
        raise
    finally:
        logger.info(f"Model duration: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    catalog = Catalog()
    model_flow(catalog)