import os
from pathlib import Path
from dotenv import load_dotenv

env = load_dotenv(override=True)

project_root = os.getenv("PROJECT_ROOT")

if not project_root:
    raise ValueError("PROJECT_ROOT is not set in .env. Please set the path to the project root folder")

ROOT_PATH = Path(project_root)

DATA_PATH = ROOT_PATH.joinpath("data")

class BaseDataPaths:
    ingest = DATA_PATH.joinpath("01_ingest")
    raw = DATA_PATH.joinpath("02_raw")
    bronze = DATA_PATH.joinpath("03_bronze")
    silver = DATA_PATH.joinpath("04_silver")
    gold = DATA_PATH.joinpath("05_gold")
    model = DATA_PATH.joinpath("06_model")
    mapping = DATA_PATH.joinpath("07_helpers", "mapping")
    logs = DATA_PATH.joinpath("98_logs")
    temp = DATA_PATH.joinpath("99_temp")
    database = DATA_PATH.joinpath("08_database")

class BronzeDataPaths:
    match_info_bronze = BaseDataPaths.bronze.joinpath("match_info_bronze.parquet")
    innings_bronze = BaseDataPaths.bronze.joinpath("innings_bronze.parquet")
    registry_bronze = BaseDataPaths.bronze.joinpath("registry_bronze.parquet")

class SilverDataPaths:
    wide_table = BaseDataPaths.silver.joinpath("wide_table.parquet")

class MappingDataPaths:
    team_league_map = BaseDataPaths.mapping.joinpath("team_league_map.csv")
    venue_map = BaseDataPaths.mapping.joinpath("venue_map.csv")

class Catalog:
    cricsheet_url = "https://cricsheet.org/downloads/all_json.zip"
    all_json_zip = BaseDataPaths.temp.joinpath("all_json.zip")
    logs = BaseDataPaths.logs
    ingest = BaseDataPaths.ingest
    raw = BaseDataPaths.raw
    bronze = BronzeDataPaths
    silver = SilverDataPaths
    mapping = MappingDataPaths
    database = BaseDataPaths.database.joinpath("cricket.duckdb")


if __name__ == "__main__":
    pass