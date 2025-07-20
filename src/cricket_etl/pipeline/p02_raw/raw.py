import json
import pickle
from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.logger import Logger

logger = Logger("raw")

def convert_ingest_json_to_pkl(catalog: Catalog) -> None:
    for json_file in catalog.ingest.glob("*.json"):
        pkl_path = catalog.raw / f"{json_file.stem}.pkl"
        skipped_files = []
        if pkl_path.exists():
            skipped_files.append(pkl_path.stem)
            continue

        try:
            with open(json_file, "rb") as f:
                data = json.load(f)

            with open(pkl_path, "wb") as pf:
                pickle.dump(data, pf)

            logger.info(f"Converted {json_file.name} to {pkl_path.name}. Existing files skipped = {len(skipped_files)}")
        except Exception as e:
            logger.error(f"Failed to process {json_file.name}: {e}")
    logger.info("Conversion from json to pkl completed")


if __name__ == "__main__":
    convert_ingest_json_to_pkl(catalog=Catalog())