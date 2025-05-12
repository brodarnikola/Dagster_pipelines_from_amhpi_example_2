# Source code converted to Dagster assets
# Date: 2025-04-16
# Additional dependencies: dagster, psycopg2-binary

import dagster
from dagster import asset, AssetExecutionContext, define_asset_job
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

@asset
def csv_file_input():
    """
    Read data from username.csv file
    """
    return pd.read_csv("username.csv", sep=";").convert_dtypes()

@asset
def filtered_data(csv_file_input: pd.DataFrame):
    """
    Filter rows based on condition - names containing 'ra'
    """
    return csv_file_input[csv_file_input['First name'].str.contains("ra", na=False)]

@asset
def postgres_output(filtered_data: pd.DataFrame):
    """
    Write filtered data to Postgres database
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
            name="simple_csv",
            con=engine,
            if_exists="append",
            index=False,
            schema=POSTGRES_SCHEMA
        )
    finally:
        engine.dispose()

# Define a job that will execute the assets
simple_pipeline_job = define_asset_job(
    name="simple_pipeline_job",
    selection=["csv_file_input", "filtered_data", "postgres_output"]
)

# This makes the assets discoverable by Dagster
defs = dagster.Definitions(
    assets=[csv_file_input, filtered_data, postgres_output],
    jobs=[simple_pipeline_job]
)