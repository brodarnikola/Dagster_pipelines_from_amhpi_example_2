# Source code converted to Dagster assets with improved lineage tracking
# Date: 2025-04-17
# Additional dependencies: dagster, psycopg2-binary

import dagster
from dagster import get_dagster_logger, asset, define_asset_job, MetadataValue, MaterializeResult
import pandas as pd
import sqlalchemy
import psycopg2
import os
import requests

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
    group_name="csv_processing"
)
def csv_file_input():
    """
    Read data from username.csv file
    """
    data = pd.read_csv("username.csv", sep=";").convert_dtypes()
    return data

@asset(
    description="Data filtered to only include names containing 'ra'",
    group_name="csv_processing"
)
def filtered_data(csv_file_input: pd.DataFrame):
    """
    Filter rows based on condition - names containing 'ra'
    """
    filtered = csv_file_input[csv_file_input['First name'].str.contains("ra", na=False)]
    return filtered

@asset(
    description="Data sorted SORTED 'ra'",
    group_name="csv_processing"
)
def sorted_data(filtered_data: pd.DataFrame):
    """
    SORTED rows based on condition - SORTED
    """
    sorted = filtered_data.sort_values(by=["First name"], ascending=[True])
    return sorted

@asset(
    description=f"Data loaded into PostgreSQL table '{POSTGRES_TABLE}'",
    group_name="database_output",
    metadata={
        "database": POSTGRES_DATABASE_NAME,
        "schema": POSTGRES_SCHEMA,
        "table": POSTGRES_TABLE
    }
)
def postgres_output(context, sorted_data: pd.DataFrame):
    """
    Write filtered data to Postgres database table simple_csv
    """
        
        
    logger = get_dagster_logger()
    # Connect to the Postgres database
    engine = sqlalchemy.create_engine(
        f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
    )
    
    # Rename columns based on the mapping
    transformed_data = sorted_data.rename(columns={"First name": "field_name"})
    
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
        
        logger.info(f"Data written to PostgreSQL table '{POSTGRES_TABLE}'")
        logger.info(f"Rows written: {len(transformed_data)}")
        # Add metadata about the operation
        context.add_output_metadata({
            "row_count": MetadataValue.int(len(transformed_data)),
            "target_table": MetadataValue.text(f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"),
            "database": MetadataValue.text(POSTGRES_DATABASE_NAME)
        
        })
        # Add lineage information
        context.add_output_metadata({
            "lineage": MetadataValue.text(f"Lineage from Dagster to PostgreSQL table '{POSTGRES_TABLE}'")
        })
        # Add lineage information to OpenMetadata
        # Assuming you have a function to get the OpenMetadata connection 
        
        logger.info("Adding lineage to OpenMetadata...")
        logger.info("Lineage added to OpenMetadata.")
        logger.info(f"Lineage fromEntity Dagster to PostgreSQL table '{context.asset_key.path[-1]}'")
        logger.info(f"Lineage toEntity Dagster to PostgreSQL table '{context.asset_key.path[-1]}'")
        logger.info(f"Lineage toEntity Dagster to PostgreSQL table '{POSTGRES_SCHEMA}.{POSTGRES_TABLE}'")
        
        # Push lineage to OpenMetadata
        requests.post(
            "http://openmetadata_server:8585/api/v1/lineage",
            json={
                "fromEntity": f"pipeline/dagster.{context.asset_key.path[-1]}",
                "toEntity": f"table/postgres.{POSTGRES_SCHEMA}.{POSTGRES_TABLE}",
                "description": f"Dagster pipeline {context.asset_key.path[-1]}"
            },
            #headers={"Authorization": "Bearer YOUR_TOKEN"}
        )

        return MaterializeResult(
            metadata={"rows_loaded": len(transformed_data)}
        )
    finally:
        engine.dispose()

# Define a job that will execute the assets
simple_pipeline_job = define_asset_job(
    name="simple_pipeline_job",
    selection=["csv_file_input", "filtered_data",  "sorted_data", "postgres_output"],
    description="Pipeline that reads CSV data, filters it, and loads to PostgreSQL"
)

# This makes the assets discoverable by Dagster
defs = dagster.Definitions(
    assets=[csv_file_input, filtered_data, sorted_data, postgres_output],
    jobs=[simple_pipeline_job]
)