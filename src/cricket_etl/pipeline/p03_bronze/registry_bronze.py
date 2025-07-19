import polars as pl

from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.functions import Functions as F
from cricket_etl.helpers.logger import Logger

logger = Logger("bronze")


def extract_registry(match_id: str, registry_dict: dict) -> list:
    """Extracts the match registry information into a Polars DataFrame."""
    extracted_registry = [
        (match_id, person_name, person_id) 
        for person_name, person_id in registry_dict.items()
    ]
    return extracted_registry


def get_registry_df(catalog: Catalog) -> pl.DataFrame:
    """Extracts match registry information into a Polars DataFrame."""

    all_registry_rows = []

    for raw_file in catalog.raw.glob("*.pkl"):
        try:
            registry = F.read_pickle(raw_file)["info"]["registry"]["people"]
            rows = extract_registry(raw_file.stem, registry)
            all_registry_rows.extend(rows)
        except Exception as e:
            logger.warning(f"Failed to extract registry from {raw_file.stem}: {e}")

    df_registry = pl.DataFrame(all_registry_rows, schema=["match_id", "person_name", "person_id"])

    logger.info(f"Registry extraction completed for {len(df_registry)} people")

    return df_registry


def write_registry(df_registry: pl.DataFrame, catalog: Catalog) -> None:
    df_registry.write_parquet(catalog.bronze.registry_bronze)
    logger.info("Registry write completed")


if __name__ == "__main__":
    catalog = Catalog()
    df_registry = get_registry_df(catalog)
    write_registry(df_registry, catalog)