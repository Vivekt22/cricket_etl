import time
from cricket_etl.helpers.catalog import Catalog
from cricket_etl.pipeline.p03_bronze.match_info_bronze import get_match_info_df, write_match_info
from cricket_etl.pipeline.p03_bronze.innings_bronze import get_innigns_df, write_innings
from cricket_etl.pipeline.p03_bronze.registry_bronze import get_registry_df, write_registry
from cricket_etl.helpers.logger import Logger

logger = Logger("bronze")

def bronze_flow(catalog: Catalog):
    start = time.time()
    try:
        df_match_info = get_match_info_df(catalog)
        write_match_info(df_match_info, catalog)

        df_innings = get_innigns_df(catalog)
        write_innings(df_innings, catalog)

        df_registry = get_registry_df(catalog)
        write_registry(df_registry, catalog)

        logger.info("Bronze flow completed")
    except Exception as e:
        logger.error(f"Bronze flow failed: {e}")
        raise
    finally:
        logger.info(f"Bronze duration: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    catalog = Catalog()
    bronze_flow(catalog)