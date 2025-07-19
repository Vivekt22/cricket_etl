import pickle
from cricket_etl.helpers.catalog import Catalog

catalog = Catalog()

class Functions:
    @staticmethod
    def read_pickle(file):
        with open(file, 'rb') as f:
            return pickle.load(f)
        
    @staticmethod
    def clear_logs():
        log_dir = catalog.logs
        for log_file in log_dir.glob("*.log"):
            with open(log_file, "w"):
                pass  # This effectively truncates the file