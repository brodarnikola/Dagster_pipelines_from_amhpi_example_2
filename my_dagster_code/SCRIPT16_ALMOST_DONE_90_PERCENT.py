# Source code converted to Dagster assets with improved lineage tracking
# Date: 2025-04-17
# Additional dependencies: dagster, psycopg2-binary

import dagster
from dagster import asset, define_asset_job, MetadataValue, MaterializeResult, get_dagster_logger, AssetSelection
import pandas as pd
import psycopg2
from io import StringIO

# OpenMetadata imports
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
    AuthProvider,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.entity.data.pipeline import Pipeline, Task
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.entityLineage import EntitiesEdge

# Connection constants for Postgres
POSTGRES_HOST = "psqldb"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE_NAME = "ecdwh"
POSTGRES_USERNAME = "bruno"
POSTGRES_PASSWORD = "bruno"
POSTGRES_SCHEMA = "public"
POSTGRES_TABLE = "simple_csv"

# OpenMetadata Configuration
JWT_TOKEN = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NDc2Njg2NzMsImV4cCI6bnVsbH0.vJWjU8gC-9v52zr2mTXwyiih_rt_BwgxzpkEzo_Ke1tAl65UH_RCRaURrSCM7LABMwIjmmDeMAwbA1nWYG7sPx5IQKdN_rXDI1SFruo2p0YqdKQMtA662j2kTzzUWJ9PvKzAD8osWPbt0aS_HctQV8SxrFXpZkvFbYsrc-lqlq0pcLIUd14KxZB_bge8HLolORAu5sA9QRJPYnqtUwixbTgynWlJE31IS9t-0z03JOenjJc7pbycluzdndi-KYQyeeMaud-M2Ph6jBlP49JjIu7P9t24W9hcDB-8H4oUGPApwqSqgEl2FTMbuxgO9I_o5-_A8cy6QUGGHUS61RcWLw"
OM_SERVER_URL = "http://openmetadata-server:8585/api"

def init_openmetadata_client():
    """Initialize and return OpenMetadata client with JWT auth"""
    security_config = OpenMetadataJWTClientConfig(jwtToken=JWT_TOKEN)
    server_config = OpenMetadataConnection(
        hostPort=OM_SERVER_URL,
        securityConfig=security_config,
        authProvider=AuthProvider.openmetadata,
    )
    return OpenMetadata(server_config)

@asset(
    description="YES 3, raw data extracted from username.csv file",
    group_name="csv_processing"
)
def csv_file_input():
    """Read data from username.csv file"""
    return pd.read_csv("username.csv", sep=";").convert_dtypes()

@asset(
    description="Filter data to only include names containing 'e'",
    group_name="csv_processing"
)
def filtered_data(csv_file_input: pd.DataFrame):
    """Filter rows based on condition - names containing 'e'"""
    return csv_file_input[csv_file_input['First name'].str.contains("e", na=False)]

@asset(
    description="Data sorted by 'First name'",
    group_name="csv_processing"
)
def sorted_data(filtered_data: pd.DataFrame):
    """Sort rows by 'First name' column"""
    return filtered_data.sort_values(by=["First name"], ascending=[True])

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
    """Write filtered data to Postgres and track lineage in OpenMetadata"""
    logger = get_dagster_logger()
    transformed_data = sorted_data.rename(columns={"First name": "field_name"})[["field_name"]]
    
    try:
        # Database operations
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DATABASE_NAME,
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD
        )
        
        cursor = conn.cursor()
        data_tuples = [tuple(x) for x in transformed_data.to_numpy()]
        insert_query = f"INSERT INTO {POSTGRES_SCHEMA}.{POSTGRES_TABLE} (field_name) VALUES (%s)"
        psycopg2.extras.execute_batch(cursor, insert_query, data_tuples)
        conn.commit()
        
        # OpenMetadata lineage tracking
        metadata = init_openmetadata_client()
        metadata.health_check()
        
        # Create pipeline entity
        pipeline_request = CreatePipelineRequest(
            name="E_Control_Storm_Gas_PIPELINE1",
            service="dagster_service",
            tasks=[
                Task(name="csv_file_input"),
                Task(name="filtered_data"),
                Task(name="sorted_data"),
                Task(name="postgres_output")
            ]
        )
        pipeline_entity = metadata.create_or_update(pipeline_request)
        
        # Get table references (you'll need to replace these with actual UUIDs)
        source_table = metadata.get_by_name(
            entity="table",
            fqn=f"{POSTGRES_DATABASE_NAME}.{POSTGRES_SCHEMA}.file_data"  # Replace with actual source
        )
        target_table = metadata.get_by_name(
            entity="table",
            fqn=f"{POSTGRES_DATABASE_NAME}.{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"
        )
        
        # Create lineage edges if tables exist
        if source_table:
            metadata.add_lineage(AddLineageRequest(
                edge=EntitiesEdge(
                    fromEntity=EntityReference(id=source_table.id, type="table"),
                    toEntity=EntityReference(id=pipeline_entity.id, type="pipeline")
                )
            ))
        
        if target_table:
            metadata.add_lineage(AddLineageRequest(
                edge=EntitiesEdge(
                    fromEntity=EntityReference(id=pipeline_entity.id, type="pipeline"),
                    toEntity=EntityReference(id=target_table.id, type="table")
                )
            ))
        
        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(len(transformed_data)),
                "target_table": MetadataValue.text(f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"),
                "database_connection": MetadataValue.text(
                    f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
                )
            }
        )
        
    except Exception as e:
        logger.error(f"Error in postgres_output: {str(e)}")
        raise
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()

# Job definitions
simple_pipeline_job = define_asset_job(
    name="simple_pipeline_job",
    selection=AssetSelection.assets(csv_file_input, filtered_data, sorted_data, postgres_output)
)

defs = dagster.Definitions(
    assets=[csv_file_input, filtered_data, sorted_data, postgres_output],
    jobs=[simple_pipeline_job]
)