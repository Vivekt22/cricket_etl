import zipfile
import shutil

import requests

from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.logger import Logger

logger = Logger("ingest")

def download_zip(catalog: Catalog) -> None:
    try:
        response = requests.get(catalog.cricsheet_url)
        response.raise_for_status()
        if catalog.all_json_zip.is_file():
            catalog.all_json_zip.unlink()
        with open(catalog.all_json_zip, "wb") as f:
            f.write(response.content)
        logger.info("Zip download completed")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

def empty_ingest_folder(catalog: Catalog) -> None:
    try:
        for item in catalog.ingest.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        logger.info("Ingest folder cleared")
    except Exception as e:
        logger.error(f"Failed to clear ingest folder: {e}")
        raise

def extract_jsons_from_zip(catalog: Catalog) -> None:
    try:
        catalog.ingest.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(catalog.all_json_zip , 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.json'):
                    zip_ref.extract(file_name, catalog.ingest)
        logger.info("JSON files extracted")
    except Exception as e:
        logger.error(f"Failed to extract JSON files: {e}")
        raise


def ingest(catalog: Catalog) -> None:
    download_zip(catalog)
    empty_ingest_folder(catalog)
    extract_jsons_from_zip(catalog)
    logger.info("Ingest completed")


if __name__ == "__main__":
    catalog = Catalog()
    ingest(catalog)