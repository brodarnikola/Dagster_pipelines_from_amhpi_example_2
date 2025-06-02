# Source code converted to Dagster assets with improved lineage tracking
# Date: 2025-04-17
# Additional dependencies: dagster, psycopg2-binary

import dagster
from dagster import asset, define_asset_job, MetadataValue, MaterializeResult, get_dagster_logger
import pandas as pd
#import sqlalchemy
import psycopg2
#import os

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

@asset(
    description="YES 3, raw data extracted from username.csv file",
    group_name="csv_processing"
)
def csv_file_input():
    """
    Awesome function 3, read data from username.csv file
    """
    data = pd.read_csv("username.csv", sep=";").convert_dtypes()
    return data

@asset(
    description="Awersome function 3, data filtered to only include names containing 'ra'",
    group_name="csv_processing"
)
def filtered_data(csv_file_input: pd.DataFrame):
    """
    Yes 2, Filter rows based on condition - names containing 'e'
    """
    filtered_data = csv_file_input[csv_file_input['First name'].str.contains("e", na=False)]
    return filtered_data

@asset(
    description="Data sorted SORTED 'e'",
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
    Write filtered data to Postgres database table simple_csv and track lineage
    """
    
    
    logger = get_dagster_logger()
    # Rename columns based on the mapping
    transformed_data = sorted_data.rename(columns={"First name": "field_name"})
    
    # Only keep relevant columns
    transformed_data = transformed_data[["field_name"]]
    
    logger.info(f"Transformed data: {transformed_data}")
    
    # Use direct psycopg2 connection instead of SQLAlchemy
    #import psycopg2
    from io import StringIO
    
    try:
        # Create a direct connection with psycopg2
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DATABASE_NAME,
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD
        )
        
        logger.info(f"Connn: {conn}")
        
        cursor = conn.cursor()
        
        # Create cursor and execute
        #with conn.cursor() as cursor:
            
            
        logger.info(f"Cursor: {cursor}")
            
        # try:    
        #     # Create table if it doesn't exist
        #     cursor.execute(f"""
        #         CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.{POSTGRES_TABLE} (
        #             field_name VARCHAR(255)
        #         )
        #     """)
        # except (Exception, psycopg2.DatabaseError) as error:
        #     logger.error(f"Error creating table: {error}")   
        # finally:
        #     logger.info(f"Created table: ")
            
        logger.info(f"Cursor execute with creating table: {cursor}")
            
            # Use copy_from for efficient data loading
        buffer = StringIO()
        transformed_data.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
            
            
        logger.info(f"Cursor transformed_data: {transformed_data}")
        logger.info(f"Cursor buffer: {buffer}")
            
        # Convert dataframe to list of tuples for batch insert
        data_tuples = [tuple(x) for x in transformed_data.to_numpy()]
            
         # Use executemany for efficient insertion
        insert_query = f"INSERT INTO {POSTGRES_SCHEMA}.{POSTGRES_TABLE} (field_name) VALUES (%s)"
        psycopg2.extras.execute_batch(cursor, insert_query, data_tuples)
            
        conn.commit()
        logger.info("Data inserted successfully")
            
            
        logger.info(f"Cursor DONE, FINISHED: {cursor}")
        
        # Add metadata about the database operation
        result = MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(len(transformed_data)),
                "target_table": MetadataValue.text(f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"),
                "database_connection": MetadataValue.text(f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}")
            }
        )
        
        #Track lineage in OpenMetadata
        # metadata_config = {
        #     "hostPort": "http://openmetadata-server:8585/api",
        #     "authProvider": "no-auth",
        # }
        # metadata = OpenMetadata(metadata_config)

        # lineage_data = Pipeline(
        #     name="E_Control_Storm_Gas_PIPELINE1",
        #     service="dagster_service",
        #     tasks=[
        #         {"name": "csv_file_input", "upstreamTasks": [], "downstreamTasks": ["filtered_data"]},
        #         {"name": "filtered_data", "upstreamTasks": ["csv_file_input"], "downstreamTasks": ["sorted_data"]},
        #         {"name": "sorted_data", "upstreamTasks": ["filtered_data"], "downstreamTasks": ["postgres_output"]},
        #         {"name": "postgres_output", "upstreamTasks": ["sorted_data"], "downstreamTasks": []},
        #     ],
        # )

        # metadata.create_or_update(lineage_data)

        # 1. Create the security config (JWT)
        # security_config = OpenMetadataJWTClientConfig(
        #     jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NDc2NTExNzIsImV4cCI6bnVsbH0.uWJhvYENtpGIYXaynY4x8uxubVkiQAJVtcImaMNx9vMqcs_qKL4xZyKWJCI8loKtmcXsBWUwYpeivY3VRyOAHZ-b_TwiWJJp_OHJWXfEAq-EJD73g-G49fTPhcIU81JQRA55Oumt6GZTLeffCpOrCwqnZXLbef-a_bi5Z_MBDHef9Vyu9SlYn8IDKeJ7IDHYCIM75F52S3gqob7RYSpSu63SydUrBXcyq9oLdEAP0I40Qv4lhfq_lUf092teAhmrR3DOMqaR4oRmpLx8v-_DYkwNQfl0ERmeAMoMirSxVP-Y_UFsTjLswuiqgIC2lwHIuexBTgFv7EFeme6OZEsx-A"  # Get from /api/v1/system/config/jwks
        # )

        # 2. Create the client config (CORRECT FOR 1.6.1)
        security_config = OpenMetadataJWTClientConfig(
            jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NDc2NTExNzIsImV4cCI6bnVsbH0.uWJhvYENtpGIYXaynY4x8uxubVkiQAJVtcImaMNx9vMqcs_qKL4xZyKWJCI8loKtmcXsBWUwYpeivY3VRyOAHZ-b_TwiWJJp_OHJWXfEAq-EJD73g-G49fTPhcIU81JQRA55Oumt6GZTLeffCpOrCwqnZXLbef-a_bi5Z_MBDHef9Vyu9SlYn8IDKeJ7IDHYCIM75F52S3gqob7RYSpSu63SydUrBXcyq9oLdEAP0I40Qv4lhfq_lUf092teAhmrR3DOMqaR4oRmpLx8v-_DYkwNQfl0ERmeAMoMirSxVP-Y_UFsTjLswuiqgIC2lwHIuexBTgFv7EFeme6OZEsx-A"  # Get from /api/v1/system/config/jwks
        )

        server_config = OpenMetadataConnection(
            hostPort="http://openmetadata-server:8585/api",
            securityConfig=security_config,
            authProvider=AuthProvider.openmetadata,
        )

        metadata = OpenMetadata(server_config)
        metadata.health_check()

        # 4. Define your pipeline lineage
        lineage_data = Pipeline(
            name="E_Control_Storm_Gas_PIPELINE1",
            service="dagster_service",
            tasks=[
                Task(name="csv_file_input", upstreamTasks=[], downstreamTasks=["filtered_data"]),
                Task(name="filtered_data", upstreamTasks=["csv_file_input"], downstreamTasks=["sorted_data"]),
                Task(name="sorted_data", upstreamTasks=["filtered_data"], downstreamTasks=["postgres_output"]),
                Task(name="postgres_output", upstreamTasks=["sorted_data"], downstreamTasks=[]),
            ],
        )

        # 5. Push to OpenMetadata
        metadata.create_or_update(lineage_data)
        return result
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()

# @asset(
#     description=f"Data loaded into PostgreSQL table '{POSTGRES_TABLE}'",
#     group_name="database_output",
#     metadata={
#         "database": POSTGRES_DATABASE_NAME,
#         "schema": POSTGRES_SCHEMA,
#         "table": POSTGRES_TABLE
#     }
# )
# def postgres_output(context, sorted_data: pd.DataFrame):
#     """
#     Write filtered data to Postgres database table simple_csv and track lineage
#     """
#     # Create SQLAlchemy connection string
#     connection_string = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
    
#     # Rename columns based on the mapping
#     transformed_data = sorted_data.rename(columns={"First name": "field_name"})
    
#     # Only keep relevant columns
#     transformed_data = transformed_data[["field_name"]]
    
#     # Write DataFrame to Postgres using SQLAlchemy properly
#     try:
#         # Create engine with explicit SQLAlchemy usage
#         from sqlalchemy import create_engine
#         engine = create_engine(connection_string)
        
#         # Use the engine to write data
#         with engine.connect() as connection:
#             transformed_data.to_sql(
#                 name=POSTGRES_TABLE,
#                 con=connection,  # Pass connection, not engine
#                 if_exists="append",
#                 index=False,
#                 schema=POSTGRES_SCHEMA
#             )
        
#         # Add metadata about the database operation
#         result = MaterializeResult(
#             metadata={
#                 "row_count": MetadataValue.int(len(transformed_data)),
#                 "target_table": MetadataValue.text(f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE}"),
#                 "database_connection": MetadataValue.text(connection_string)
#             }
#         )
        
#         # Track lineage in OpenMetadata
#         metadata_config = {
#             "hostPort": "http://openmetadata-server:8585/api",
#             "authProvider": "no-auth",
#         }
#         metadata = OpenMetadata(metadata_config)

#         lineage_data = Pipeline(
#             name="E_Control_Storm_Gas_PIPELINE1",
#             service="dagster_service",
#             tasks=[
#                 {"name": "csv_file_input", "upstreamTasks": [], "downstreamTasks": ["filtered_data"]},
#                 {"name": "filtered_data", "upstreamTasks": ["csv_file_input"], "downstreamTasks": ["sorted_data"]},
#                 {"name": "sorted_data", "upstreamTasks": ["filtered_data"], "downstreamTasks": ["postgres_output"]},
#                 {"name": "postgres_output", "upstreamTasks": ["sorted_data"], "downstreamTasks": []},
#             ],
#         )

#         metadata.create_or_update(lineage_data)
#         return result
#     finally:
#         if 'engine' in locals():
#             engine.dispose()

from dagster import AssetSelection

simple_pipeline_job = define_asset_job(
    name="simple_pipeline_job",
    selection=AssetSelection.assets(csv_file_input, filtered_data, sorted_data, postgres_output)
)

# This makes the assets discoverable by Dagster
defs = dagster.Definitions(
    assets=[csv_file_input, filtered_data, sorted_data, postgres_output],
    jobs=[simple_pipeline_job]
)


def track_lineage(context, transformed_data):
    # 1. Create pipeline entity
    pipeline_request = CreatePipelineRequest(
        name="E_Control_Storm_Gas_PIPELINE1",
        service="dagster_service",  # Must exist in OpenMetadata
        tasks=[
            Task(name="csv_file_input"),
            Task(name="filtered_data"),
            Task(name="sorted_data"),
            Task(name="postgres_output")
        ]
    )
    pipeline_entity = metadata.create_or_update(pipeline_request)

    # 2. Create lineage edges
    lineage_request = AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(
                id="source-entity-uuid",  # Replace with your source table UUID
                type="table"
            ),
            toEntity=EntityReference(
                id=pipeline_entity.id,
                type="pipeline"
            )
        )
    )
    metadata.add_lineage(lineage_request)

    # Add downstream lineage if needed
    downstream_lineage = AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(
                id=pipeline_entity.id,
                type="pipeline"
            ),
            toEntity=EntityReference(
                id="target-entity-uuid",  # Replace with your target table UUID
                type="table"
            )
        )
    )
    metadata.add_lineage(downstream_lineage)