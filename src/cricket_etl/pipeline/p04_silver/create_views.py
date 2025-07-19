from cricket_etl.helpers.catalog import Catalog
import duckdb as db
from cricket_etl.helpers.logger import Logger

logger = Logger("silver")


def create_views(catalog: Catalog):
    """
    Creates DuckDB views using file paths from the catalog.
    
    Parameters:
        con (duckdb.DuckDBPyConnection): The DuckDB connection.
        catalog (dict): The catalog containing file paths.
    """
    
    # Connect to database
    con = db.connect(catalog.database)

    con.sql("use wide")

    # Create view for innings
    con.sql(
        f"""--sql
        create or replace view v_innings as
        select *
        from read_parquet('{catalog.bronze.innings_bronze}')
        """
    )

    # Create view for info
    con.sql(
        f"""--sql
        create or replace view v_info as
        select *
        from read_parquet('{catalog.bronze.match_info_bronze}')
        """
    )

    # Create view for registry
    con.sql(
        f"""--sql
        create or replace view v_registry as
        select *
        from read_parquet('{catalog.bronze.registry_bronze}')
        """
    )

    # Create view for team league map
    con.sql(
        f"""--sql
        create or replace view v_team_league_map as
        select * 
        from read_csv('{catalog.mapping.team_league_map}')
        """
    )

    con.sql(
        f"""--sql
        create or replace view v_venue_map as
        select venue, ground, city, country
        from read_csv('{catalog.mapping.venue_map}')
        """
    )

    con.sql("checkpoint cricket")
    con.close()

    logger.info("Create views completed")