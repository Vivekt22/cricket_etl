import duckdb as db
from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.logger import Logger

logger = Logger("silver")

def create_schema(catalog: Catalog):
    con = db.connect(catalog.database)
    try:
        con.sql("create schema if not exists wide")
        con.sql("create schema if not exists model")
        con.sql("checkpoint cricket")
    finally:
        con.close()

    logger.info("Create schema completed")