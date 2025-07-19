import json
import pickle
from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.logger import Logger

logger = Logger("raw")

def convert_ingest_json_to_pkl(catalog: Catalog) -> None:
    for json_file in catalog.ingest.glob("*.json"):
        with open(json_file, "rb") as f:
            data = json.load(f)
        
        pkl_path = catalog.raw / f"{json_file.stem}.pkl"
        with open(pkl_path, "wb") as pf:
            pickle.dump(data, pf)

    logger.info("Conversion from json to pkl completed")            


if __name__ == "__main__":
    convert_ingest_json_to_pkl(catalog=Catalog())