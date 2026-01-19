# Cricket Data Processing Pipeline

## Overview

The Cricket Data Processing Pipeline is a modular and scalable framework designed to extract, transform, and load cricket match data from raw files into structured formats optimized for analysis and reporting. The pipeline supports layered data processing, from ingestion to model-ready outputs, and is tailored for efficient performance using Polars and DuckDB. Outputs are intended for business intelligence tools such as Power BI.

---

## Project Structure

```plaintext
.
├── README.md                  # Project documentation
├── pyproject.toml             # Project dependencies and configuration
├── uv.lock                    # Lock file for dependencies
├── .env, .gitignore, etc.     # Environment and Git settings
├── data                       # Directory containing all data layers
│   ├── 02_raw                 # Unprocessed data extracted from source
│   ├── 03_bronze              # Parsed but unnormalized data (match info, innings, registry)
│   ├── 04_silver              # Wide-table format for fast querying
│   ├── 05_model               # Dimension and fact tables for Power BI
│   ├── 06_helpers             # Mapping and tracking files
│   │   ├── mapping            # Team/venue/league mapping
│   │   └── tracker            # Ingestion and processing logs
│   ├── 07_database            # DuckDB database file
│   ├── 98_logs                # Runtime logs
│   └── 99_temp                # Temporary files
├── src
│   └── cricket_etl            # Main source code package
│       ├── __main__.py        # Entry point for running the pipeline
│       ├── helpers            # Utilities for cataloging, logging, and DuckDB
│       ├── notebooks          # Jupyter notebooks for testing and prototyping
│       └── pipeline           # ETL pipeline modules organized by layer
│           ├── p01_ingest     # Ingest files to raw folder
│           ├── p02_raw        # Convert raw pickle to structured Polars tables
│           ├── p03_bronze     # Extract match info, innings data, registries
│           ├── p04_silver     # Create wide tables and views
│           └── p05_model      # Create dimension and fact tables
└── tests                      # Unit tests for helpers and pipelines
```

---

## Pipeline Layers

### **1. Ingest Layer (`p01_ingest`)**

- Extracts zipped raw data into a structured folder layout.
- Responsible for ensuring all source files are ready for downstream processing.

### **2. Raw Layer (`p02_raw`)**

- Converts match `.pkl` files into raw structured tables for match info, innings, and registries.
- Minimal parsing or standardization.

### **3. Bronze Layer (`p03_bronze`)**

- Extracts meaningful structured data:
  - `match_info_bronze.py`: match metadata
  - `innings_bronze.py`: batting and bowling data
  - `registry_bronze.py`: player/team registry

### **4. Silver Layer (`p04_silver`)**

- `create_schema.py`: defines DuckDB schema.
- `create_views.py`: materializes intermediate views.
- `create_wide_table.py`: wide-table combining all relevant match stats.

### **5. Model Layer (`p05_model`)**

- `create_dim_tables.py`: dimension tables (players, venues, teams).
- `create_fact_tables.py`: fact tables (match outcomes, innings summaries).
- `write_model_parquets.py`: exports final tables to Parquet for BI consumption.

---

## Running the Pipeline

### **Full Pipeline Execution**

To run the complete ETL process:

```bash
uv run cricket-etl
```

### **Individual Stage Execution**

To run individual stages:

```bash
uv run src/cricket_etl/pipeline/p01_ingest/ingest_flow.py
uv run src/cricket_etl/pipeline/p02_raw/raw_flow.py
uv run src/cricket_etl/pipeline/p03_bronze/bronze_flow.py
uv run src/cricket_etl/pipeline/p04_silver/silver_flow.py
uv run src/cricket_etl/pipeline/p05_model/model_flow.py
```

---

## Tech Stack

- **Python**: Core development language
- **Polars**: Fast DataFrame engine for ETL
- **DuckDB**: Embedded analytical database
- **Parquet**: Efficient columnar storage
- **Power BI**: Visualization and dashboarding
- **UV**: Dependency management and execution

---

## Next Enhancements

- Parallelism across layers for speedup
- Data validation checks and alerting
- More complete metadata and data lineage
- Version tracking for raw and processed files

---

This project is designed to efficiently process and structure cricket data at scale, ready for downstream analytics and reporting.