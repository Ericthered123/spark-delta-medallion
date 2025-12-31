data/ -- here the dataset will be contained

data/raw/ -- Entry CSVs , raw data. Raw data is kept inmutable to preserve lineage

data/delta/ -- delta lake tables and bronze/silver/gold , 
            its purpose is to separate raw data from processed data.

pipelines/ -- business logic , spark scripts , ingestion jobs, transformations
            each script is a job , ecah pipeline is implemented as an indepent spark job

schemas/ -- data contract, explicit schemas and type definition.
            schemas act as contracts between pipeline stages.

utils/ -- shared infraestructure, SparkSession creation , common helpers,config
        Shared utilities ensure consistent Spark config across jobs.


venv/ -- Python virtual env with isolated dependencies, its key because it enables
        consistency , reliability and avoids conflicts between projects.

requirements.txt -- versioned dependencies 

