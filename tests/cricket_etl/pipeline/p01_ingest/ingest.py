import zipfile
import os

import requests

from cricket_etl.helpers.catalog import Catalog
from cricket_etl.helpers.logger import Logger

logger = Logger("ingest")

def download_zip(catalog: Catalog) -> None:
    # Download the file
    response = requests.get(catalog.cricsheet_url)
    response.raise_for_status()  # Raise an error for bad responses

    # Delete old file if exists
    if catalog.all_json_zip.is_file():
        catalog.all_json_zip.unlink()

    # Save the file
    with open(catalog.all_json_zip, "wb") as f:
        f.write(response.content)

    logger.info("Zip download completed")

def empty_ingest_folder(catalog: Catalog) -> None:
    # Clear the ingest folder
    for item in catalog.ingest.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            os.rmdir(item)
    
    logger.info("Ingest folder cleared")

def extract_jsons_from_zip(catalog: Catalog) -> None:
    # Extract only JSON files
    with zipfile.ZipFile(catalog.all_json_zip , 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith('.json'):
                zip_ref.extract(file_name, catalog.ingest)

    logger.info("JSON files extracted")


def ingest(catalog: Catalog) -> None:
    download_zip(catalog)
    empty_ingest_folder(catalog)
    extract_jsons_from_zip(catalog)
    logger.info("Ingest completed")


if __name__ == "__main__":
    catalog = Catalog()
    ingest(catalog)