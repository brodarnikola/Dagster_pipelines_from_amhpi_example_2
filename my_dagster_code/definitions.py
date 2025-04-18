import psycopg2  # For PostgreSQL interaction
import json
import pandas as pd
import hashlib  # For hash calculation
from dotenv import load_dotenv
from dagster import (
    job,
    op,
    sensor,
    RunRequest,
    get_dagster_logger,
    Definitions,
    DefaultRunLauncher,
    run_status_sensor,
    RunStatusSensorContext,
    SkipReason,
    DagsterRunStatus,
)

from datetime import datetime
from dagster_aws.s3 import S3Resource  # , s3_list_objects
from typing import Union, List, Tuple, Dict
from io import BytesIO
import os
import re
from NETWORK_OPERATOR import process_file_job


load_dotenv(dotenv_path="configs.env")  # Create this file

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")  # Simplified endpoint URL
DB_PASSWORD = os.getenv("DB_PASSWORD")  # Simplified endpoint URL

s3_resource = S3Resource(
    endpoint_url="http://minio:9000",
    aws_access_key_id="root",  # Replace with your actual Minio username
    aws_secret_access_key="password",  # Replace with your actual Minio password
    region_name=None,
    # Include any other configuration you need
)


@op(required_resource_keys={"s3"})
def fetch_and_parse_files_from_minio(context, new_files):
    """Fetches and parses all Excel and CSV files from Minio S3."""
    logger = get_dagster_logger()
    all_files_data = []
    s3 = context.resources.s3

    if not new_files:
        context.log.info("No new files to process")
        return None

    context.log.info(f"Processing files: {new_files}")

    try:
        for file_path in new_files:
            file_name = file_path.split("/")[-1]
            file_extension = file_name.split(".")[-1].lower()
            file_path_local = f"/tmp/{file_name}"

            # Download file using chunks
            with open(file_path_local, "wb") as local_file:
                response = s3.get_object(Bucket="ecvw", Key=file_path)
                stream = response["Body"]
                chunk_size = 8192  # 8KB chunks
                while True:
                    chunk = stream.read(chunk_size)
                    if not chunk:
                        break
                    local_file.write(chunk)

            file_hash = calculate_sha256_hash(file_path_local)
            logger.info(f"File hash data '{file_hash}'.")

            # Choose the correct parser based on file extension
            if file_extension in ["xls", "xlsx"]:
                file_data_for_current_file = parse_excel_file(file_path_local, file_name, logger)
            elif file_extension == "csv":
                file_data_for_current_file = parse_csv_file(file_path_local, file_name, logger)
            else:
                logger.warning(f"Unsupported file type: {file_extension}. Skipping {file_name}.")
                continue

            if file_data_for_current_file:
                all_files_data.append(
                    {
                        "file_name": file_name,
                        "file_path": file_path,
                        "file_hash": file_hash,
                        "file_data": file_data_for_current_file,
                    }
                )

        logger.info(f"Data fetched and parsed for {len(all_files_data)} files.")
        return all_files_data

    except Exception as e:
        logger.error(f"Error fetching files: {e}")
        raise

def parse_excel_file(file_path_local, file_name, logger):
    """Parses an Excel file and extracts sheet data."""
    try:
        file_data = []
        excel_file = pd.ExcelFile(file_path_local, engine="openpyxl")
        sheet_names = excel_file.sheet_names

        for sheet_name in sheet_names:
            try:
                excel_data = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl")
                logger.info(f"Fetched sheet '{sheet_name}' from {file_name}")

                cell_data = {}
                for col in excel_data.columns:
                    for row_num, value in excel_data[col].items():
                        if pd.isna(value):
                            continue
                        elif value is not None and str(value).lower() != "null":
                            cell_key = f"{chr(65 + excel_data.columns.get_loc(col))}{row_num + 2}"
                            if isinstance(value, (datetime, pd.Timestamp)):
                                value = value.isoformat()
                            cell_data[cell_key] = value

                if cell_data:
                    file_data.append({"sheet_name": sheet_name, "json_data": cell_data})
                else:
                    logger.info(f"No valid data found for sheet '{sheet_name}'.")

            except ValueError as e:
                logger.warning(f"Skipping sheet '{sheet_name}' in {file_name}: {e}")
                continue

        return file_data

    except Exception as e:
        logger.error(f"Error processing Excel file {file_name}: {e}")
        return None
    
def parse_csv_file(file_path_local, file_name, logger):
    """Parses a CSV file and extracts structured data as a list of dictionaries."""
    try:
        csv_data = pd.read_csv(file_path_local, dtype=str)  # Read CSV as strings
        logger.info(f"Fetched CSV data from {file_name}")

        if csv_data.empty:
            logger.info(f"No valid data found in CSV file '{file_name}'.")
            return None

        # Convert CSV data to a list of dictionaries (natural row structure)
        structured_data = csv_data.to_dict(orient="records")
        
        # Another way to structure the data (cell-based) .. (key- value parsing) key is the cell position, and value is the cell value
        # structured_data = {}
        # for col in csv_data.columns:
        #     for row_num, value in csv_data[col].items():
        #         if pd.isna(value):
        #             continue
        #         elif value is not None and str(value).lower() != "null":
        #             cell_key = f"{chr(65 + csv_data.columns.get_loc(col))}{row_num + 2}"
        #             structured_data[cell_key] = value

        return [{"csv_data": structured_data}] if structured_data else None 

    except Exception as e:
        logger.error(f"Error processing CSV file {file_name}: {e}")
        return None

def check_file_duplicate(file_hash: str) -> bool:
    """Checks if a file with the given hash already exists in the database."""
    logger = get_dagster_logger()

    logger.info(f"look for duplicate file_hash '{file_hash}' .")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM File WHERE hash = %s", (file_hash,))
        existing_file = cur.fetchone()

        logger.info(f"existing_file '{existing_file}' fetched.")
        cur.close()
        conn.close()

        return existing_file is not None  # Return True if duplicate, False otherwise

    except psycopg2.Error as e:
        logger.error(f"Database error while checking duplicate: {e}")
        return (
            False  # Assume not a duplicate in case of an error (better safe than sorry)
        )


def calculate_sha256_hash(file_input: Union[str, BytesIO, bytes]) -> str:
    """
    Calculate SHA256 hash of a file or file-like object.

    Args:
        file_input: Can be a file path (str), BytesIO object, or bytes

    Returns:
        str: The calculated SHA256 hash
    """
    sha256_hash = hashlib.sha256()

    if isinstance(file_input, (str, os.PathLike)):
        # Handle file path
        with open(file_input, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
    elif isinstance(file_input, BytesIO):
        # Handle BytesIO object
        for chunk in iter(lambda: file_input.read(4096), b""):
            sha256_hash.update(chunk)
        file_input.seek(0)  # Reset position for future reads
    elif isinstance(file_input, bytes):
        # Handle bytes directly
        sha256_hash.update(file_input)
    else:
        raise TypeError(f"Unsupported input type: {type(file_input)}")

    return sha256_hash.hexdigest()


@op(required_resource_keys={"s3"})
def write_data_to_postgres(context, all_files_data: list):
    """Save Excel & CSV data into the database and return sheet names for the next job."""
    logger = get_dagster_logger()
    s3 = context.resources.s3
    file_info = []  # Collects objects with file_id and sheet_names

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for file_data in all_files_data:
            file_name = file_data["file_name"]
            file_extension = file_name.split(".")[-1].lower()
            file_data_list = file_data["file_data"]
            file_hash = file_data["file_hash"]
            file_path = file_data["file_path"]

            try:
                conn.autocommit = False
                
                # Extract 'Gas' or 'Strom' from the filename
                energy_type = extract_energy_type(file_name)
                logger.info(f"Energy type is: '{energy_type}' ")

                # Insert into File table
                cur.execute(
                    "INSERT INTO File (file_name, hash, file_events, energy) VALUES (%s, %s, %s, %s) RETURNING id",
                    (file_name, file_hash, "Processing", energy_type),
                )
                file_id = cur.fetchone()[0]
                logger.info(f"File '{file_name}' inserted with ID {file_id}.")

                if file_id:
                    if file_extension in ["xls", "xlsx"]:
                        file_entry = save_excel_data(cur, file_id, file_name, file_data_list, logger)
                        file_info.append(file_entry)
                    elif file_extension == "csv":
                        save_csv_data(cur, file_id, file_data_list)

                move_to_archive(s3, file_path, file_name, logger)
                conn.commit()
                logger.info(f"Transaction for file '{file_name}' committed.")

            except psycopg2.Error as e:
                conn.rollback()
                logger.error(f"Transaction error for file '{file_name}': {e}")
                raise
            finally:
                conn.autocommit = True

        cur.close()
        conn.close()
        logger.info("Data loaded to PostgreSQL successfully.")

        context.log.info(f"FILE_INFO_TAG:{json.dumps(file_info)}")
        return file_info

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL connection error: {e}")
        raise

def extract_energy_type(file_name):
    """Extracts 'Gas' or 'Strom' from the filename."""
    match = re.search(r'_(Gas|Strom)_', file_name)
    return match.group(1) if match else "Unknown"

def save_excel_data(cur, file_id, file_name, file_data_list, logger):
    """Saves Excel sheet data into PostgreSQL."""
    file_entry = {
        "file_id": file_id,
        "file_name": file_name,
        "sheet_names": []
    }

    for item in file_data_list:
        sheet_name = item["sheet_name"]
        json_data = item["json_data"]

        file_entry["sheet_names"].append(sheet_name)

        cur.execute(
            "INSERT INTO File_data (file_id, sheet_name, json_data) VALUES (%s, %s, %s)",
            (file_id, sheet_name, json.dumps(json_data)),
        )

    return file_entry

def save_csv_data(cur, file_id, file_data_list):
    """Saves CSV data into PostgreSQL."""
    for item in file_data_list:
        csv_rows = item["csv_data"]  # This is now a list of dictionaries (rows)
        
        cur.execute(
            "INSERT INTO File_data (file_id, sheet_name, json_data) VALUES (%s, %s, %s)",
            (file_id, None, json.dumps(csv_rows)),  # Store CSV as raw row-based JSON .. every row is separately parsed, every row is a dictionary
        )

def check_file_in_database(file_hash: str, conn) -> bool:
    """Check if file with given hash exists in database."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM File 
            WHERE hash = %s AND file_events = 'Success'
        """,
            (file_hash,),
        )
        return cur.fetchone() is not None


def move_to_archive(s3, file_path: str, file_name: str, logger):
    """Move file to archive bucket."""
    try:
        s3.copy_object(
            CopySource={"Bucket": "ecvw", "Key": file_path},
            Bucket="ecvw-archiv",
            Key=file_name,
        )
        s3.delete_object(Bucket="ecvw", Key=file_path)
        logger.info(f"File '{file_name}' moved to archive.")
    except Exception as e:
        logger.error(f"Error moving file to archive: {e}")
        raise


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
    # return psycopg2.connect(
    #     dbname="ecdwh", user="bruno", password="bruno", host="psqldb", port="5432"
    # )


@job
def download_file_job():
    """Dagster job to fetch and parse Excel data."""
    excel_data = fetch_and_parse_files_from_minio()
    write_data_to_postgres(excel_data)


@sensor(
    job=download_file_job,
    name="download_file_sensor",
    description="Sensor to detect new files in S3",
    required_resource_keys={"s3"},
    minimum_interval_seconds=10,
)
def download_file_sensor(context):
    """Checks for new files in S3 and verifies against database."""
    try:
        logger = context.log
        s3 = context.resources.s3

        # Test S3 connection
        try:
            logger.info("FFFF Testing connection by listing buckets...")
            buckets = s3.list_buckets()
            logger.info(
                f"Successfully connected to S3. Buckets: {[b['Name'] for b in buckets['Buckets']]}"
            )
        except Exception as e:
            logger.error(f"Failed to list buckets: {str(e)}")
            raise e

        response = s3.list_objects_v2(Bucket="ecvw", Prefix="")
        logger.info(f"Successfully listed objects in bucket 'ecvw'")

        if "Contents" not in response:
            return

        # Connect to database
        conn = get_db_connection()

        try:
            current_files = []
            for obj in response["Contents"]:
                file_path = obj["Key"]
                file_name = file_path.split("/")[-1]

                # Download file content
                response = s3.get_object(Bucket="ecvw", Key=file_path)
                file_content = response["Body"].read()

                # Calculate hash from content directly
                file_hash = calculate_sha256_hash(file_content)

                # Check if file is already in database
                if check_file_in_database(file_hash, conn):
                    logger.info(
                        f"File {file_name} already exists in database, moving to archive"
                    )
                    move_to_archive(s3, file_path, file_name, logger)
                else:
                    current_files.append(file_path)

            # Get the last run's file list from the cursor
            last_run_files = set()
            if context.cursor:
                last_run_files = set(context.cursor.split(","))

            new_files = list(set(current_files) - last_run_files)
            logger.info(f"Found {len(new_files)} new files: {new_files}")

            if new_files:
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                run_key = f"run_{timestamp}"

                yield RunRequest(
                    run_key=run_key,
                    run_config={
                        "ops": {
                            "fetch_and_parse_files_from_minio": {
                                "config": {},
                                "inputs": {"new_files": {"value": new_files}},
                            }
                        }
                    },
                )

                context.update_cursor(",".join(current_files))

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error in sensor: {e}")
        import traceback

        logger.error(traceback.format_exc())


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=process_file_job,
    monitored_jobs=[download_file_job],
    minimum_interval_seconds=10,
)
def process_file_sensor(context: RunStatusSensorContext):
    if context.dagster_run.job_name == process_file_job.name:
        return SkipReason("Skipping to prevent recursive triggering.")
    
    logger = get_dagster_logger()
    logs = context.instance.all_logs(context.dagster_run.run_id)
    
    file_info = []
    
    for log in logs:
        if log.message.startswith("FILE_INFO_TAG:"):
            file_info_str = log.message.replace("FILE_INFO_TAG:", "")
            try:
                file_info = json.loads(file_info_str)
                logger.info(f"Extracted file_info: {file_info}")
                break
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing file info JSON: {e}")
    
    if not file_info:
        context.log.warning("No file info found from logs.")
        return None
    
    context.log.info(f"Launching {process_file_job.name} with file info: {file_info}")
    
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {
                "start_file_processing": {
                    "inputs": {"file_info": file_info}
                }
            }
        },
        tags={"triggered_by": context.dagster_run.run_id},
    ) 
    
defs = Definitions(
    jobs=[download_file_job, process_file_job],
    # job_assets={parse_convert_data_job: {"depends_on": [download_file_job.to_job_asset()]}}, #Important part to add
    # job_assets={parse_convert_data_job: {"depends_on": [download_file_job.to_job_asset()]}},
    sensors=[download_file_sensor, process_file_sensor],
    resources={"s3": s3_resource}  #"io_manager": FileSheetDataManager() } # add the I/O manager},
)
