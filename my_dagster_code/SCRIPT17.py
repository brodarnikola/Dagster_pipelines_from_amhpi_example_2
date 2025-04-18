# Source code converted to Dagster assets without custom I/O manager
# Date: 2025-04-17
# Additional dependencies: dagster, psycopg2-binary

import dagster
from dagster import asset, define_asset_job, MetadataValue
import pandas as pd
import sqlalchemy
import psycopg2
import os

# Connection constants for Postgres
POSTGRES_HOST = "psqldb"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE_NAME = "ecdwh"
POSTGRES_USERNAME = "bruno"
POSTGRES_PASSWORD = "bruno"
POSTGRES_SCHEMA = "public"
POSTGRES_TABLE = "simple_csv"

@asset(
    description="Raw data extracted from username.csv file",
    metadata={
        "source": "file://username.csv",
        "schema": "CSV with semicolon delimiter"
    }
)
def csv_file_input():
    """
    Read data from username.csv file
    """
    data = pd.read_csv("username.csv", sep=";").convert_dtypes()
    return data

@asset(
    description="Data filtered to only include names containing 'ra'",
    metadata={
        "filter_criteria": "First name contains 'ra'"
    }
)
def filtered_data(csv_file_input: pd.DataFrame):
    """
    Filter rows based on condition - names containing 'ra'
    """
    filtered = csv_file_input[csv_file_input['First name'].str.contains("ra", na=False)]
    return filtered

@asset(
    description=f"Data loaded into PostgreSQL table '{POSTGRES_TABLE}' with transformed column names",
    metadata={
        "database": POSTGRES_DATABASE_NAME,
        "schema": POSTGRES_SCHEMA,
        "table": POSTGRES_TABLE,
        "columns": ["field_name"],
        "connection": f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}",
        "destination": f"{POSTGRES_DATABASE_NAME}.{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"
    },
    key_prefix=[POSTGRES_DATABASE_NAME, POSTGRES_SCHEMA]
)
def postgres_output(context, filtered_data: pd.DataFrame):
    """
    Write filtered data to Postgres database table simple_csv
    """
    # Connect to the Postgres database
    engine = sqlalchemy.create_engine(
        f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
    )
    
    # Rename columns based on the mapping
    transformed_data = filtered_data.rename(columns={"First name": "field_name"})
    
    # Only keep relevant columns
    transformed_data = transformed_data[["field_name"]]
    
    # Write DataFrame to Postgres
    try:
        transformed_data.to_sql(
            name=POSTGRES_TABLE,
            con=engine,
            if_exists="append",
            index=False,
            schema=POSTGRES_SCHEMA
        )
        
        # Add metadata about the operation
        context.add_output_metadata({
            "row_count": MetadataValue.int(len(transformed_data)),
            "target_table": MetadataValue.text(f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"),
            "database": MetadataValue.text(POSTGRES_DATABASE_NAME)
        })
        
    finally:
        engine.dispose()

# Define a job that will execute the assets
simple_pipeline_job = define_asset_job(
    name="simple_pipeline_job",
    selection=["csv_file_input", "filtered_data", ["ecdwh", "public", "postgres_output"]],
    description="Pipeline that reads CSV data, filters it, and loads to PostgreSQL"
)

# This makes the assets discoverable by Dagster
defs = dagster.Definitions(
    assets=[csv_file_input, filtered_data, postgres_output],
    jobs=[simple_pipeline_job]
)