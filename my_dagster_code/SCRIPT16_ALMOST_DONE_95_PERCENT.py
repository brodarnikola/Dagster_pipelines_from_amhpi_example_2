# Source code converted to Dagster assets with improved lineage tracking
# Date: 2025-04-17
# Additional dependencies: dagster, psycopg2-binary

import dagster
from dagster import asset, define_asset_job, MetadataValue, MaterializeResult, get_dagster_logger, AssetSelection
import pandas as pd
import psycopg2
import psycopg2.extras
from io import StringIO
import json

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
from metadata.generated.schema.api.services.createPipelineService import CreatePipelineServiceRequest
from metadata.generated.schema.entity.services.pipelineService import PipelineService, PipelineServiceType
from metadata.generated.schema.entity.services.connections.pipeline.airflowConnection import AirflowConnection

# Connection constants for Postgres
POSTGRES_HOST = "psqldb"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE_NAME = "ecdwh"
POSTGRES_USERNAME = "bruno"
POSTGRES_PASSWORD = "bruno"
POSTGRES_SCHEMA = "public"
POSTGRES_TABLE = "simple_csv"

# OpenMetadata Configuration
JWT_TOKEN = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImFkbWluIiwicm9sZXMiOlsiQWRtaW4iXSwiZW1haWwiOiJhZG1pbkBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90IjpmYWxzZSwidG9rZW5UeXBlIjoiUEVSU09OQUxfQUNDRVNTIiwiaWF0IjoxNzQ3NzM4MTk3LCJleHAiOjE3NTAzMzAxOTd9.MQPQLVvBjQyzT2oudsIloVAJeK5KowLnB0Hao4onzKk--0VUDqEex9aNyPdeuRVRBVxSF4SL5qi5rButcaZK4APzNAS6hak59sGqGuEzMou0dn5qgGCsLb0nPmCw2iOXl8v9qlO-tbPkUllTPKMOUEf1aeVCDtCVhjhB1Rm8HHpsigR7et0JZ8AWajGTKM-ezQTwVxPz8km5JTLA4jQMxGGlpytySFdk7ob7FrMotLjklEBBK8kIC7kuHIJhzWxVe5_bHPeX-CQUXix-8fVzIblHterbaxiz-0lSCbsJusZg-977morIQVeB2sazswgbMyatKkk3s5PPhwINcFsZeA"
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

def create_pipeline_service(metadata_client):
    """Create a Airflow pipeline service in OpenMetadata if it doesn't exist"""
    logger = get_dagster_logger()
    
    try:
        # Check if service already exists
        existing_service = metadata_client.get_by_name(
            entity=PipelineService,
            fqn="dagster_service"
        )
        
        if existing_service:
            logger.info("Pipeline service 'dagster_service' already exists")
            return existing_service
        
        # Create service if it doesn't exist
        # Note: Using Airflow service type since Dagster isn't directly supported in this version
        pipeline_service_json = {
            "name": "dagster_service",
            "serviceType": PipelineServiceType.Airflow,
            "connection": {
                "config": {
                    "type": "Airflow",
                    "hostPort": "http://localhost:8080",
                    "connection": {
                        "type": "Backend"
                    }
                }
            }
        }
        
        create_pipeline_service_entity = CreatePipelineServiceRequest(**pipeline_service_json)
        pipeline_service_entity = metadata_client.create_or_update(create_pipeline_service_entity)
        logger.info(f"Created pipeline service: {pipeline_service_entity.name}")
        return pipeline_service_entity
    except Exception as e:
        logger.error(f"Error creating pipeline service: {str(e)}")
        raise

@asset(
    description="Raw data extracted from username.csv file",
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
        
        # Create pipeline service first (if not exists)
        pipeline_service = create_pipeline_service(metadata)
        
        # Create pipeline entity
        pipeline_request = CreatePipelineRequest(
            name="E_Control_Storm_Gas_PIPELINE2",
            displayName="E-Control Storm Gas Pipeline 2",
            description="Pipeline for processing gas data 2",
            service=pipeline_service.fullyQualifiedName,
            tasks=[
                Task(name="csv_file_input", description="Extract from CSV"),
                Task(name="filtered_data", description="Filter rows containing 'e'"),
                Task(name="sorted_data", description="Sort by first name"),
                Task(name="postgres_output", description="Load to Postgres")
            ]
        )
        
        pipeline_entity = metadata.create_or_update(pipeline_request)
        logger.info(f"Created pipeline: {pipeline_entity.name}")
        
        # Get table references (you'll need to replace these with actual UUIDs)
        try:
            source_table = metadata.get_by_name(
                entity="table",
                fqn=f"{POSTGRES_DATABASE_NAME}.{POSTGRES_SCHEMA}.file"  # Replace with actual source
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
                logger.info("Created source lineage")
            
            if target_table:
                metadata.add_lineage(AddLineageRequest(
                    edge=EntitiesEdge(
                        fromEntity=EntityReference(id=pipeline_entity.id, type="pipeline"),
                        toEntity=EntityReference(id=target_table.id, type="table")
                    )
                ))
                logger.info("Created target lineage")
        except Exception as le:
            logger.warning(f"Could not create lineage: {str(le)}")
        
        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(len(transformed_data)),
                "target_table": MetadataValue.text(f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"),
                "database_connection": MetadataValue.text(
                    f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
                ),
                "pipeline_created": MetadataValue.text(f"pipeline_entity:{pipeline_entity} ")
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