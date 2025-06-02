"""
Integration example: Dagster pipeline with OpenMetadata lineage tracking
"""

import dagster
from dagster import asset, define_asset_job, MetadataValue, MaterializeResult
import pandas as pd
import sqlalchemy
import os

# For OpenMetadata
from metadata.generated.schema.entity.data.pipeline import Pipeline
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.ingestion.ometa.ometa_api import OpenMetadata

# Connection constants for Postgres
POSTGRES_HOST = "psqldb"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE_NAME = "ecdwh"
POSTGRES_USERNAME = "bruno"
POSTGRES_PASSWORD = "bruno"
POSTGRES_SCHEMA = "public"
POSTGRES_TABLE = "simple_csv"

# OpenMetadata connection (modify URL as needed)
OPENMETADATA_HOST = "http://openmetadata-server:8585/api"

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
    filtered_data = csv_file_input[csv_file_input['First name'].str.contains("ra", na=False)]
    return filtered_data

@asset(
    description="Data sorted by first name",
    group_name="csv_processing"
)
def sorted_data(filtered_data: pd.DataFrame):
    """
    Sort rows based on first name
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
    Write filtered data to Postgres database table simple_csv and track lineage
    """
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
        
        # Track lineage in OpenMetadata (wrapped in try/except to prevent failures)
        try:
            # Initialize OpenMetadata client
            metadata_config = OpenMetadataConnection(hostPort=OPENMETADATA_HOST)
            metadata = OpenMetadata(metadata_config)
            
            # Define pipeline with lineage information
            lineage_data = Pipeline(
                name="simple_csv_pipeline",
                displayName="Simple CSV Processing Pipeline",
                description="Pipeline that processes CSV data and loads to PostgreSQL",
                service="dagster_service",  # Must match an existing service name in OpenMetadata
                tasks=[
                    {"name": "csv_file_input", "upstreamTasks": [], "downstreamTasks": ["filtered_data"]},
                    {"name": "filtered_data", "upstreamTasks": ["csv_file_input"], "downstreamTasks": ["sorted_data"]},
                    {"name": "sorted_data", "upstreamTasks": ["filtered_data"], "downstreamTasks": ["postgres_output"]},
                    {"name": "postgres_output", "upstreamTasks": ["sorted_data"], "downstreamTasks": []},
                ],
            )
            
            # Create or update the pipeline definition (creates lineage)
            metadata.create_or_update(lineage_data)
            context.log.info("OpenMetadata lineage created successfully")
            
        except Exception as e:
            context.log.error(f"Failed to create OpenMetadata lineage: {str(e)}")
            # Don't fail the asset operation if metadata tracking fails
        
        # Add metadata about the database operation
        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(len(transformed_data)),
                "target_table": MetadataValue.text(f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"),
                "database_connection": MetadataValue.text(f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}")
            }
        )
    finally:
        engine.dispose()

# Define a job that will execute the assets
simple_pipeline_job = define_asset_job(
    name="simple_pipeline_job",
    selection=["csv_file_input", "filtered_data", "sorted_data", "postgres_output"],
    description="Pipeline that reads CSV data, filters it, and loads to PostgreSQL"
)

# This makes the assets discoverable by Dagster
defs = dagster.Definitions(
    assets=[csv_file_input, filtered_data, sorted_data, postgres_output],
    jobs=[simple_pipeline_job]
)