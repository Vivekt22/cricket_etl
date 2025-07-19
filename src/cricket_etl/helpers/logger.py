import logging
from pathlib import Path
from cricket_etl.helpers.catalog import Catalog

catalog = Catalog()

class Logger:
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.log_dir = catalog.logs
        self.logger = self._setup_logger(self.log_dir)

    def _setup_logger(self, log_dir: Path) -> logging.Logger:
        log_file = log_dir / f"{self.pipeline_name}.log"

        logger = logging.getLogger(self.pipeline_name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def info(self, message: str):
        self.logger.info(f"{self.pipeline_name.upper()}: {message}")

    def warning(self, message: str):
        self.logger.warning(f"{self.pipeline_name.upper()}: {message}")

    def error(self, message: str):
        self.logger.error(f"{self.pipeline_name.upper()}: {message}")