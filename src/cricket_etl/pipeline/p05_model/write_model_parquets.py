from cricket_etl.helpers.catalog import Catalog
import duckdb as db
from cricket_etl.helpers.logger import Logger

logger = Logger("model")

def write_model_parquets(catalog: Catalog):
    # Connect to database
    con = db.connect(catalog.database)
    
    try:
        model_tables = con.sql(
            """--sql
            select
                'model.' || table_name as table_name
            from information_schema.tables
            where
                table_schema = 'model'
            """
        ).df().values.flatten()

        logger.info(f"Found {len(model_tables)} tables in schema 'model'.")

        for table in model_tables:
            file_name = table.split(".")[-1] + ".parquet"
            file_path = catalog.model.joinpath(file_name)
            
            con.sql(
                f"""--sql
                copy (select * from {table})
                to '{file_path}'
                """
            )

    except Exception as e:
        logger.error(f"Write model parquets failed: {e}")
        raise

    finally:
        con.close()

    logger.info("All model tables written to Parquet successfully.")
