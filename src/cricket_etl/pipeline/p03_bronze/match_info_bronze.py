import polars as pl

from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.functions import Functions as F
from cricket_etl.helpers.logger import Logger

logger = Logger("bronze")

def extract_match_info(match_id: str, match_info_dict: dict) -> dict:
    extracted_data = {
            "match_id": match_id,
            "balls_per_over": match_info_dict.get("balls_per_over"),
            "city": match_info_dict.get("city"),
            "start_date": match_info_dict.get("dates", [None])[0],
            "end_date": match_info_dict.get("dates", [None])[-1],
            "event": match_info_dict.get("event", {}).get("name"),
            "gender": match_info_dict.get("gender"),
            "match_type": match_info_dict.get("match_type"),
            "match_referee": match_info_dict.get("officials", {}).get(
                "match_referees", [None]
            )[0],
            "umpire_01": match_info_dict.get("officials", {}).get("umpires", [None])[0],
            "umpire_02": match_info_dict.get("officials", {}).get("umpires", [None])[-1],
            "winner": match_info_dict.get("outcome", {}).get("winner"),
            "winner_awarded": match_info_dict.get("outcome", {}).get("winner"),
            "win_by": next(iter(match_info_dict.get("outcome", {}).get("by", {})), None),
            "win_margin": (
                next(reversed(list(match_info_dict.get("outcome", {}).get("by", {}).values())))
                if match_info_dict.get("outcome", {}).get("by")
                else None
            ),
            "result": match_info_dict.get("outcome", {}).get("result"),
            "default_overs": match_info_dict.get("overs"),
            "player_of_match": (
                match_info_dict.get("player_of_match", [None])[0]
                if isinstance(match_info_dict.get("player_of_match"), list)
                else match_info_dict.get("player_of_match")
            ),
            "season": str(match_info_dict.get("season")).split("/")[0],
            "team_type": match_info_dict.get("team_type"),
            "team_01": match_info_dict.get("teams", [None, None])[0],
            "team_02": match_info_dict.get("teams", [None, None])[1],
            "toss_winner": match_info_dict.get("toss", {}).get("winner"),
            "toss_decision": match_info_dict.get("toss", {}).get("decision"),
            "venue": match_info_dict.get("venue"),
        }

    return extracted_data


def get_match_info_df(catalog: Catalog) -> pl.DataFrame:
    """Extracts match information into a Polars DataFrame."""
    
    df_match_info = pl.DataFrame([
        extract_match_info(
            raw_file.stem, 
            F.read_pickle(raw_file)["info"]
        )
        for raw_file in catalog.raw.glob("*.pkl")
    ])

    df_match_info = df_match_info.cast(
        {
            "balls_per_over": pl.Int64,
            "start_date": pl.Date,
            "end_date": pl.Date,
            "win_margin": pl.Int64,
            "default_overs": pl.Int64,
            "season": pl.Int64
        }
    )

    match_count = len(df_match_info)

    logger.info(f"Match info extraction completed for {match_count} matches")
    
    return df_match_info


def write_match_info(df_match_info: pl.DataFrame, catalog: Catalog) -> None:
    df_match_info.write_parquet(catalog.bronze.match_info_bronze)
    logger.info("Match info write completed")


if __name__ == "__main__":
    catalog = Catalog()
    df_match_info = get_match_info_df(catalog)
    write_match_info(df_match_info, catalog)