import polars as pl
from polars import col, lit

from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.functions import Functions as F
from cricket_etl.helpers.logger import Logger

logger = Logger("bronze")

def flatten_struct(
    df: pl.DataFrame,
    column_name: str,
    rename_map: dict = None, # type: ignore
    exclude: list = None, # type: ignore
) -> pl.DataFrame:
    """
    Description:
    ------------
    Flattens a struct column by extracting its fields into new columns.

    To avoid naming conflicts (for example, if a field's new alias would duplicate an existing column),
    this function extracts fields into temporary columns, drops the original struct column, and then
    renames the temporary columns. If a new alias already exists, a numeric suffix is appended.

    Args:
    -----
    - df (pl.DataFrame): Input DataFrame.
    - column_name (str): The struct column to flatten.
    - rename_map (dict, optional): A dict mapping original field names to desired new column names. Defaults to None.
    - exclude (list, optional): A list of field names to skip. Defaults to None.

    Returns:
    --------
    - pl.DataFrame: DataFrame with the struct column flattened.
    """
    if column_name not in df.columns:
        return df

    rename_map = rename_map or {}
    exclude = exclude or []

    # Get field names from the first non-null row (if available)
    sample = df[column_name].drop_nulls().to_list()
    if sample:
        keys = list(sample[0].keys())
    else:
        keys = []

    keys = [k for k in keys if k not in exclude]
    temp_aliases = {}
    for key in keys:
        # Determine desired new column name.
        new_key = rename_map.get(key, key)
        # Ensure new_key does not conflict with existing columns or with names we've already assigned.
        base_new_key = new_key
        counter = 1
        while new_key in df.columns or new_key in temp_aliases.values():
            new_key = f"{base_new_key}_{counter}"
            counter += 1

        temp_alias = f"__temp_{column_name}_{key}"
        df = df.with_columns(pl.col(column_name).struct.field(key).alias(temp_alias))
        temp_aliases[temp_alias] = new_key

    df = df.drop(column_name)
    df = df.rename(temp_aliases)
    return df


def extract_innings(match_id: str, inning: dict, inning_num: int) -> pl.DataFrame:
    """
    Processes one inning's data: explodes overs/deliveries, flattens nested fields,
    and adds metadata columns.
    """
    if "overs" not in inning:
        return None  # type: ignore

    # Create a DataFrame from the overs list.
    df_inning = pl.DataFrame(inning["overs"])
    # Explode deliveries in each over.
    df_inning = df_inning.explode("deliveries")
    # Flatten the deliveries struct (exclude "over" if present).
    df_inning = flatten_struct(df_inning, "deliveries", exclude=["over"])
    # Add row index
    df_inning = df_inning.with_row_index(name="innings_row_index", offset=1)
    # Flatten the runs struct with renaming.
    df_inning = flatten_struct(
        df_inning, "runs", rename_map={"batter": "batter_runs", "total": "total_runs"}
    )
    # Process extrast if present.
    if "extras" in df_inning.columns and df_inning["extras"].dtype == pl.Struct:
        df_inning = df_inning.unnest("extras")
    # Process wickets if present.
    if "wickets" in df_inning.columns:
        df_inning = df_inning.explode("wickets")
        df_inning = flatten_struct(df_inning, "wickets")
    # Process fielders if present.
    if "fielders" in df_inning.columns:
        df_inning = df_inning.explode("fielders")
        df_inning = flatten_struct(
            df_inning, "fielders", rename_map={"name": "fielder", "kind": "wicket_type"}
        )
    # Add metadata columns.
    df_inning = df_inning.with_columns(
        [
            lit(match_id).alias("match_id"),
            lit(inning_num + 1).alias("inning_num"),
            lit(inning.get("team")).alias("batting_team"),
            lit(inning.get("declared")).alias("declared"),
            (lit(match_id) + "_" + lit(inning_num + 1).cast(pl.String)).alias(
                "innings_id"
            ),
        ]
    ).with_columns(
        ball=col("innings_row_index")
        .cum_count()
        .over("innings_id", "over", order_by="innings_row_index")
    )

    # Optionally drop unwanted columns.
    for unwanted in ["review", "non_boundary", "replacements", "substitute"]:
        if unwanted in df_inning.columns:
            df_inning = df_inning.drop(unwanted)
    return df_inning


def get_innigns_df(catalog: Catalog) -> pl.DataFrame:
    """Extracts and flattens all innings data into a single Polars DataFrame."""
    raw_files = list(catalog.raw.glob("*.pkl"))

    innings_frames = []
    for raw_file in raw_files:
        match_id = raw_file.stem
        innings = F.read_pickle(raw_file)["innings"]
        for inning_num, inning in enumerate(innings):
            if "overs" in inning:
                try:
                    df_inning = extract_innings(match_id, inning, inning_num)
                    if df_inning is not None:
                        innings_frames.append(df_inning)
                except Exception as e:
                    print(f"Error processing match {match_id}, inning {inning_num}: {e}")
                    raise e

    df_innings = (
        pl.concat(innings_frames, how="diagonal_relaxed")
        if innings_frames
        else pl.DataFrame()
    )

    delivery_count = len(df_innings)

    logger.info(f"Innings extraction completed for {delivery_count} deliveries")

    return df_innings

def write_innings(df_innings: pl.DataFrame, catalog: Catalog) -> None:
    df_innings.write_parquet(catalog.bronze.innings_bronze)
    logger.info("Innings write completed")


if __name__ == "__main__":
    catalog = Catalog()
    df_innings = get_innigns_df(catalog)
    write_innings(df_innings, catalog)