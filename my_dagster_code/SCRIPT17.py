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


 headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENMETADATA_JWT_TOKEN}"
    }


NEW CORRECT JWT TOKEN
"eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImFkbWluIiwicm9sZXMiOlsiQWRtaW4iXSwiZW1haWwiOiJhZG1pbkBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90IjpmYWxzZSwidG9rZW5UeXBlIjoiUEVSU09OQUxfQUNDRVNTIiwiaWF0IjoxNzQ3MjM0NTU2LCJleHAiOjE3NTUwMTA1NTZ9.SmbWLy5A_tw0SqA8_273w00BatYEP1eAVXVbDm6FCCC4kz52JXgxAnGkg6wHXbPT5ImCsPvqXOWd_vRHTE2dk0IAK4cz6fsLTp1rNg813kmjDrBOrTG-EGIIo9ddxsbekb-cysTXHz2Pzs0xAviGmS8wCNQQrMvlwFoNEMtlBSbhVKfl6xGEzUp8KLea7YYjjjtoqGjWAXu8VUFwbztOg1Ga0A1sK0xlsxNZu9MCMhbr1eF8DtHePfrWlsYCo8p6biy9mk5Q0IWBQ4oiyBlzucYPbjpMoIa4jC2OAN-gn5SxM3NaUb2MyYwVW7vQ_84OxZeAftmWK2I8eMxtChGGfg"


"eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NDcyMjUyNzYsImV4cCI6bnVsbH0.cglke6pILQPjDEyDQZD4hlXNS5XjoICmcRMQD1VY3QogDMLK4kfbrOrq_zEORAkabioVomna9hDGjObB0EqYR-AAMmUCiHuHI5HJz4LQXEtcU5zT-Pk1j4EqOsjWL-T9z5gU5aB_J3eja_MlZo58J48TRX8ikUcV0r0gV_buNj4qaBMg71k6otD5Tlirq0XBTotXLFslII6U_VQceDsSTXgAq1nSWXW0P7MnZLW0_RHdTzyuU8tzpPX2JLtzhRD9B-Z_MhK-tlgHxoO0KaNMmDGkLE-l6KhO6iz0FW9S4jeCDWLO9seWj6ydKZQrU3s_J5J8FX26jelMSaDwhD9a2A"

from dagster import AssetSelection

simple_pipeline_job = define_asset_job(
    name="simple_pipeline_job",
    selection=AssetSelection.assets(csv_file_input, filtered_data, sorted_data, postgres_output)
)