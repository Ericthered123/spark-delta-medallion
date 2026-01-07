# Incremental Data Pipeline with Spark & Delta Lake

A batch–incremental data pipeline implementing Medallion Architecture (Bronze–Silver–Gold) using PySpark and Delta Lake.

##  Project Objective

The goal of this project is to simulate a realistic event ingestion system and design an **incremental batch data pipeline** using Spark and Delta Lake.

The focus is not on data volume, but on:
- Incremental processing
- Idempotent writes
- Clear data layering
- Correct use of Delta Lake patterns

- ## Project Story

This project simulates an application that generates daily event files (CSV).
Each file represents **new data arriving on a given day**.

The pipeline is designed so that:
- Previously processed data is not reprocessed
- Running the pipeline multiple times produces the same result
- Each layer has a clear responsibility

- ## Dataset

Input data consists of CSV files located in `data/raw/`.

Example:
- events_2025-01-01.csv
- events_2025-01-02.csv

Each file represents a new batch of events and is processed incrementally.



## Architecture Overview

The pipeline follows the Medallion Architecture pattern:

Raw CSV → Bronze (Delta) → Silver (Delta) → Gold (Delta)

Each layer is stored as a Delta table and has a specific responsibility.


## 1 Bronze Layer — Raw Ingestion

### Responsibilities
- Ingest raw CSV files
- Append-only writes
- Preserve original data
- Explicit schema definition

### Incremental Strategy
- A processing metadata table tracks which files were already ingested
- Only new files are processed
- Re-running the job does not duplicate data

### Key Decisions
- Append instead of overwrite
- No transformations (raw data)
- Delta Lake for reliability and versioning



## 2 Silver Layer — Clean & Curated Data

### Responsibilities
- Type casting
- Null handling
- Deduplication
- Incremental processing

### Idempotency
- Data is merged using natural keys
- Running the pipeline multiple times produces the same result

### Key Concepts Demonstrated
- Incremental batch processing
- Delta MERGE
- Data quality enforcement



## 3 Gold Layer — Analytics Ready Data

### Responsibilities
- Aggregate curated data
- Produce business-level metrics
- Optimize for analytics consumption

### Examples
- Events per day
- Events by type

### Writing Strategy
- Controlled overwrite
- Small, aggregated datasets


##  Data Contracts

Each layer enforces a clear data contract:

- Bronze:
  - Raw schema
  - No guarantees on data quality
- Silver:
  - Cleaned schema
  - Non-null mandatory fields
  - Deduplicated records
- Gold:
  - Aggregated, analytics-ready schema


##  Incremental Processing

The pipeline is incremental, not full reprocessing.

When a new CSV file arrives:
- Bronze ingests only new files
- Silver processes only new Bronze records
- Gold recomputes metrics based on curated data

This makes the pipeline:
- Efficient
- Cost-aware
- Production-oriented



##  Idempotency

The pipeline is designed to be idempotent.

If any job is executed multiple times:
- No duplicated data is produced
- Results remain consistent

This is achieved through:
- Processing metadata
- Delta MERGE operations
- Natural keys



## Testing

Basic unit tests are implemented using pytest to validate:
- Metadata-based incremental logic
- Business rules and assumptions

Tests focus on correctness of rules rather than data volume.



## 🛠️ Tech Stack

- Python
- PySpark
- Delta Lake
- pytest

##  How to Run

1. Add new CSV files to `data/raw/`
2. Run Bronze ingestion
3. Run Silver transformation
4. Run Gold aggregation



## Why This Project Matters

This project demonstrates real-world data engineering concepts:
- Incremental batch pipelines
- Idempotent processing
- Delta Lake best practices
- Clear data modeling using Medallion Architecture

It is designed as a learning and portfolio project, reflecting production-oriented thinking rather than toy examples.
