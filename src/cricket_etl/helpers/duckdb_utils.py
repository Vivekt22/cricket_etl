import duckdb
import shutil
import os
from pathlib import Path

class DuckDBUtils:
    @staticmethod
    def vacuum_database(database_path: Path):
        """
        Creates a compact copy of the DuckDB database to remove bloat,
        deletes the original, and replaces it with the compacted copy.
        """
        assert database_path.exists(), f"Database file does not exist: {database_path}"
        assert database_path.is_file(), f"Expected a file, got: {database_path}"

        # Define copy path
        temp_copy_path = database_path.with_name("copy.duckdb")

        try:
            duckdb.execute(f"ATTACH '{database_path}' AS db1;")
            duckdb.execute(f"ATTACH '{temp_copy_path}' AS db2;")
            duckdb.execute("COPY FROM DATABASE db1 TO db2;")

            os.remove(database_path)

            compacted_db_file = temp_copy_path.parent / (database_path.name)
            shutil.move(str(temp_copy_path), str(compacted_db_file))

            print(f"Vacuum completed: {compacted_db_file}")
        except Exception as e:
            print(f"Vacuum failed: {e}")
            raise


if __name__ == "__main__":
    from cricket_etl.helpers.catalog import Catalog
    catalog = Catalog()
    DuckDBUtils.vacuum_database(catalog.database)