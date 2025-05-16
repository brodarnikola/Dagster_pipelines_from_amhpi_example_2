import dagster
from dagster import asset, define_asset_job, MetadataValue, MaterializeResult
import pandas as pd
import sqlalchemy
import psycopg2
import os
import requests
import json
from typing import Dict, Any, List

# Connection constants for Postgres
POSTGRES_HOST = "psqldb"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE_NAME = "ecdwh"
POSTGRES_USERNAME = "bruno"
POSTGRES_PASSWORD = "bruno"
POSTGRES_SCHEMA = "public"
POSTGRES_TABLE = "simple_csv"

# OpenMetadata connection settings
OPENMETADATA_HOST = "http://openmetadata-server:8585"
OPENMETADATA_API_ENDPOINT = f"{OPENMETADATA_HOST}/api"

# Use JWT token for admin user with proper permissions
# Note: You should store this securely in environment variables in production
OPENMETADATA_JWT_TOKEN = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImFkbWluIiwicm9sZXMiOlsiQWRtaW4iXSwiZW1haWwiOiJhZG1pbkBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90IjpmYWxzZSwidG9rZW5UeXBlIjoiUEVSU09OQUxfQUNDRVNTIiwiaWF0IjoxNzQ3MjM0NTU2LCJleHAiOjE3NTUwMTA1NTZ9.SmbWLy5A_tw0SqA8_273w00BatYEP1eAVXVbDm6FCCC4kz52JXgxAnGkg6wHXbPT5ImCsPvqXOWd_vRHTE2dk0IAK4cz6fsLTp1rNg813kmjDrBOrTG-EGIIo9ddxsbekb-cysTXHz2Pzs0xAviGmS8wCNQQrMvlwFoNEMtlBSbhVKfl6xGEzUp8KLea7YYjjjtoqGjWAXu8VUFwbztOg1Ga0A1sK0xlsxNZu9MCMhbr1eF8DtHePfrWlsYCo8p6biy9mk5Q0IWBQ4oiyBlzucYPbjpMoIa4jC2OAN-gn5SxM3NaUb2MyYwVW7vQ_84OxZeAftmWK2I8eMxtChGGfg"

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
    description="Data sorted by 'First name'",
    group_name="csv_processing"
)
def sorted_data(filtered_data: pd.DataFrame):
    """
    Sort rows based on first name
    """
    sorted_df = filtered_data.sort_values(by=["First name"], ascending=[True])
    return sorted_df

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
        
        # Create lineage in OpenMetadata using REST API directly instead of SDK
        try:
            create_lineage_in_openmetadata(context)
            context.log.info(f"Successfully created lineage in OpenMetadata for {POSTGRES_SCHEMA}.{POSTGRES_TABLE}")
        except Exception as e:
            context.log.error(f"Failed to create lineage in OpenMetadata: {str(e)}")
        
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

def create_lineage_in_openmetadata(context):
    """
    Create lineage in OpenMetadata using REST API directly
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENMETADATA_JWT_TOKEN}"
    }
    
    # First verify connection to OpenMetadata
    try:
        verify_response = requests.get(
            f"{OPENMETADATA_API_ENDPOINT}/v1/system/config",
            headers=headers
        )
        if verify_response.status_code != 200:
            context.log.error(f"Cannot connect to OpenMetadata API: {verify_response.status_code} - {verify_response.text}")
            return
    except Exception as e:
        context.log.error(f"Failed to connect to OpenMetadata: {str(e)}")
        return
    
    # Get entity IDs from OpenMetadata
    try:
        # Get or create pipeline service and entity
        pipeline_service_id = get_or_create_pipeline_service(context, headers)
        if not pipeline_service_id:
            return
            
        pipeline_id = get_or_create_pipeline_entity(context, headers, pipeline_service_id)
        if not pipeline_id:
            return
            
        # Get or create database service and table entity
        db_service_id = get_or_create_database_service(context, headers)
        if not db_service_id:
            return
            
        table_id = get_or_create_table_entity(context, headers, db_service_id)
        if not table_id:
            return
        
        # Create lineage
        create_lineage_edge(context, headers, pipeline_id, table_id)
        
    except Exception as e:
        context.log.error(f"Error in lineage creation process: {str(e)}")

def get_or_create_pipeline_service(context, headers):
    """Get or create pipeline service in OpenMetadata"""
    service_name = "dagster_service"
    
    try:
        # Try to get existing service
        service_response = requests.get(
            f"{OPENMETADATA_API_ENDPOINT}/v1/services/pipelineServices/name/{service_name}",
            headers=headers
        )
        
        if service_response.status_code == 200:
            context.log.info(f"Found existing pipeline service: {service_name}")
            return service_response.json()["id"]
        
        # Create pipeline service
        service_data = {
            "name": service_name,
            "serviceType": "Dagster",
            "description": "Dagster pipeline service",
            "connection": {
                "config": {
                    "type": "Dagster",
                    "hostPort": "http://dagster-daemon:8080",
                    "configSource": {
                        "securityConfig": "noAuth"
                    }
                }
            }
        }
        
        context.log.info(f"Creating pipeline service: {service_name}")
        create_response = requests.post(
            f"{OPENMETADATA_API_ENDPOINT}/v1/services/pipelineServices",
            headers=headers,
            json=service_data
        )
        
        if create_response.status_code not in [200, 201]:
            context.log.error(f"Failed to create pipeline service: {create_response.status_code} - {create_response.text}")
            return None
            
        return create_response.json()["id"]
    except Exception as e:
        context.log.error(f"Error in get_or_create_pipeline_service: {str(e)}")
        return None

def get_or_create_pipeline_entity(context, headers, service_id):
    """Get or create pipeline entity in OpenMetadata"""
    pipeline_name = "E_Control_Storm_Gas_PIPELINE1"
    service_name = "dagster_service"
    
    try:
        # Try to get existing pipeline
        pipeline_response = requests.get(
            f"{OPENMETADATA_API_ENDPOINT}/v1/pipelines/name/{service_name}.{pipeline_name}",
            headers=headers
        )
        
        if pipeline_response.status_code == 200:
            context.log.info(f"Found existing pipeline: {pipeline_name}")
            return pipeline_response.json()["id"]
        
        # Create pipeline entity
        pipeline_data = {
            "name": pipeline_name,
            "displayName": "Gas Pipeline ETL",
            "description": "Pipeline that reads CSV data, filters it, and loads to PostgreSQL",
            "pipelineUrl": f"http://dagster-daemon:8080/pipelines/simple_pipeline_job",
            "tasks": [
                {"name": "csv_file_input", "displayName": "Extract CSV", "description": "Read data from CSV"},
                {"name": "filtered_data", "displayName": "Filter Data", "description": "Filter rows containing 'ra'"},
                {"name": "sorted_data", "displayName": "Sort Data", "description": "Sort by first name"},
                {"name": "postgres_output", "displayName": "Load to Postgres", "description": "Load to PostgreSQL"}
            ],
            "service": {
                "id": service_id,
                "type": "pipelineService"
            }
        }
        
        context.log.info(f"Creating pipeline entity: {pipeline_name}")
        create_response = requests.post(
            f"{OPENMETADATA_API_ENDPOINT}/v1/pipelines",
            headers=headers,
            json=pipeline_data
        )
        
        if create_response.status_code not in [200, 201]:
            context.log.error(f"Failed to create pipeline entity: {create_response.status_code} - {create_response.text}")
            return None
            
        return create_response.json()["id"]
    except Exception as e:
        context.log.error(f"Error in get_or_create_pipeline_entity: {str(e)}")
        return None

def get_or_create_database_service(context, headers):
    """Get or create database service in OpenMetadata"""
    service_name = "postgres_service"
    
    try:
        # Try to get existing service
        service_response = requests.get(
            f"{OPENMETADATA_API_ENDPOINT}/v1/services/databaseServices/name/{service_name}",
            headers=headers
        )
        
        if service_response.status_code == 200:
            context.log.info(f"Found existing database service: {service_name}")
            return service_response.json()["id"]
        
        # Create database service
        service_data = {
            "name": service_name,
            "serviceType": "Postgres",
            "description": "PostgreSQL database service",
            "connection": {
                "config": {
                    "type": "Postgres",
                    "hostPort": f"{POSTGRES_HOST}:{POSTGRES_PORT}",
                    "username": POSTGRES_USERNAME,
                    "password": POSTGRES_PASSWORD,
                    "database": POSTGRES_DATABASE_NAME,
                    "connectionOptions": {},
                    "connectionArguments": {}
                }
            }
        }
        
        context.log.info(f"Creating database service: {service_name}")
        create_response = requests.post(
            f"{OPENMETADATA_API_ENDPOINT}/v1/services/databaseServices",
            headers=headers,
            json=service_data
        )
        
        if create_response.status_code not in [200, 201]:
            context.log.error(f"Failed to create database service: {create_response.status_code} - {create_response.text}")
            return None
            
        return create_response.json()["id"]
    except Exception as e:
        context.log.error(f"Error in get_or_create_database_service: {str(e)}")
        return None

def get_or_create_table_entity(context, headers, service_id):
    """Get or create table entity in OpenMetadata"""
    service_name = "postgres_service"
    fqn = f"{service_name}.{POSTGRES_DATABASE_NAME}.{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"
    
    try:
        # Try to get existing table
        table_response = requests.get(
            f"{OPENMETADATA_API_ENDPOINT}/v1/tables/name/{fqn}",
            headers=headers
        )
        
        if table_response.status_code == 200:
            context.log.info(f"Found existing table: {fqn}")
            return table_response.json()["id"]
        
        # Get database entity or create it if it doesn't exist
        database_fqn = f"{service_name}.{POSTGRES_DATABASE_NAME}"
        database_response = requests.get(
            f"{OPENMETADATA_API_ENDPOINT}/v1/databases/name/{database_fqn}",
            headers=headers
        )
        
        if database_response.status_code != 200:
            # Create database entity
            database_data = {
                "name": POSTGRES_DATABASE_NAME,
                "service": {
                    "id": service_id,
                    "type": "databaseService"
                }
            }
            
            database_create_response = requests.post(
                f"{OPENMETADATA_API_ENDPOINT}/v1/databases",
                headers=headers,
                json=database_data
            )
            
            if database_create_response.status_code not in [200, 201]:
                context.log.error(f"Failed to create database entity: {database_create_response.status_code} - {database_create_response.text}")
                return None
                
            database_id = database_create_response.json()["id"]
        else:
            database_id = database_response.json()["id"]
        
        # Get schema entity or create it if it doesn't exist
        schema_fqn = f"{service_name}.{POSTGRES_DATABASE_NAME}.{POSTGRES_SCHEMA}"
        schema_response = requests.get(
            f"{OPENMETADATA_API_ENDPOINT}/v1/databaseSchemas/name/{schema_fqn}",
            headers=headers
        )
        
        if schema_response.status_code != 200:
            # Create schema entity
            schema_data = {
                "name": POSTGRES_SCHEMA,
                "database": {
                    "id": database_id,
                    "type": "database"
                }
            }
            
            schema_create_response = requests.post(
                f"{OPENMETADATA_API_ENDPOINT}/v1/databaseSchemas",
                headers=headers,
                json=schema_data
            )
            
            if schema_create_response.status_code not in [200, 201]:
                context.log.error(f"Failed to create schema entity: {schema_create_response.status_code} - {schema_create_response.text}")
                return None
                
            schema_id = schema_create_response.json()["id"]
        else:
            schema_id = schema_response.json()["id"]
        
        # Create table entity
        table_data = {
            "name": POSTGRES_TABLE,
            "description": "Table created by Dagster pipeline",
            "columns": [
                {
                    "name": "field_name",
                    "dataType": "VARCHAR",
                    "description": "First name field"
                }
            ],
            "databaseSchema": {
                "id": schema_id,
                "type": "databaseSchema"
            }
        }
        
        context.log.info(f"Creating table entity: {POSTGRES_TABLE}")
        create_response = requests.post(
            f"{OPENMETADATA_API_ENDPOINT}/v1/tables",
            headers=headers,
            json=table_data
        )
        
        if create_response.status_code not in [200, 201]:
            context.log.error(f"Failed to create table entity: {create_response.status_code} - {create_response.text}")
            return None
            
        return create_response.json()["id"]
    except Exception as e:
        context.log.error(f"Error in get_or_create_table_entity: {str(e)}")
        return None

def create_lineage_edge(context, headers, from_entity_id, to_entity_id):
    """Create lineage edge between pipeline and table"""
    try:
        lineage_data = {
            "edge": {
                "fromEntity": {
                    "id": from_entity_id,
                    "type": "pipeline"
                },
                "toEntity": {
                    "id": to_entity_id,
                    "type": "table"
                },
                "lineageDetails": {
                    "pipeline": {
                        "columnsLineage": [
                            {
                                "fromColumns": [{"name": "First name"}],
                                "toColumn": {"name": "field_name"}
                            }
                        ],
                        "description": "Lineage from Dagster pipeline to Postgres table"
                    }
                }
            }
        }
        
        context.log.info(f"Creating lineage edge from pipeline {from_entity_id} to table {to_entity_id}")
        lineage_response = requests.post(
            f"{OPENMETADATA_API_ENDPOINT}/v1/lineage",
            headers=headers,
            json=lineage_data
        )
        
        if lineage_response.status_code not in [200, 201]:
            context.log.error(f"Failed to create lineage: {lineage_response.status_code} - {lineage_response.text}")
            return None
            
        context.log.info("Successfully created lineage in OpenMetadata")
        return lineage_response.json()
    except Exception as e:
        context.log.error(f"Error in create_lineage_edge: {str(e)}")
        return None

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