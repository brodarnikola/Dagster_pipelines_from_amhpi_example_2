# from dagster import job, op, get_dagster_logger, Definitions
from dagster import job, op, get_dagster_logger
import pandas as pd

# import sqlalchemy
# from definitions import get_db_connection, calculate_sha256_hash
import psycopg2  # For PostgreSQL interaction
from dotenv import load_dotenv
import os
from datetime import datetime
from typing import Union, List, Dict
from io import BytesIO
from MM_BIL import process_mm_bil_data, transform_mm_bil_data, write_to_mm_bil_table
from MENGENE_BLINDSTROM import (
    process_mengene_ev_blindstrom,
    extract_and_transform_mengene_blindstrom_data,
    write_data_to_mengene_blindstrom_table,
)
#from FileSheetData import FileSheetData

load_dotenv(dotenv_path="configs.env")  # Create this file

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")  # Simplified endpoint URL
DB_PASSWORD = os.getenv("DB_PASSWORD")  # Simplified endpoint URL


@op
def process_mm_wechsel_data(file_id: int, cur): # (file_ids: List[int]):
    logger = get_dagster_logger()
    data = []
    try:
        # conn = psycopg2.connect(
        #     host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        # )
        # cur = conn.cursor()

        logger.info("STARTING MM_Wechsel WHOLE DATA TRANSFORMATION...")
        cur.execute(
            "SELECT json_data, file_id FROM File_data WHERE sheet_name = %s and file_id = %s",
            ("MM_Wechsel", file_id),
        )
        data = cur.fetchall()
        # placeholders = ','.join(['%s'] * len(file_ids))
        # query = f"SELECT json_data, file_id FROM File_data WHERE sheet_name = %s AND file_id IN ({placeholders})"
        # params = ["MM_Wechsel"] + file_ids
        #cur.execute(query, params)
        #data = cur.fetchall()
 
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

    finally:
        logger.info("Database fetched.")
        return data

   
@op
def transform_mm_wechsel_data(data: List[tuple], energy_type: str, file_name: str) -> List[Dict]:
    logger = get_dagster_logger()
    logger.info("Starting MM_Wechsel data transformation... for file: " + file_name)

    transformed_rows = []

    if not data or not data[0][0]:
        logger.error("No data provided for MM_Wechsel transformation")
        return []

    # Define column mappings based on energy type
    if energy_type == "Gas":
        start_month = "E"  # Gas starts at column E
    else:
        start_month = "F"  # Other energy types start at column F 
    
    keys_to_extract = {}

    if( energy_type == "Gas"):
        # Both of this for loops, works good for file "004"
        for i in range(1652, 1666):  # 1707 inclusive
            keys_to_extract[str(i)] = 11 + (i - 1652)
        
        for i in range(1666, 1861):  # 1712 inclusive
            keys_to_extract[str(i)] = 35 + (i - 1666)    
    else:
        # Both of this for loops, works good for file "001"
        for i in range(1, 17):  # 1707 inclusive
            keys_to_extract[str(i)] = 11 + (i - 1)

        # Generate keys dynamically from 1710 to 1712
        for i in range(17, 904):  # 1712 inclusive
            keys_to_extract[str(i)] = 43 + (i - 17) 

    # Process each row in the input data
    for item in data:
        json_data = item[0]  # Extract JSON dictionary
        file_id = item[1]  # Extract file_id

        #year = json_data.get("Q9")  # Extract year
        #if not year:
        #    logger.error(f"No valid year found in JSON: {json_data}")
        #    continue  # Skip processing this row if year is missing

        # Iterate over each key and generate 12 rows (one per month)
        for key, row_number in keys_to_extract.items():
            for month_num in range(1, 13):
                month_column = chr(ord(start_month) + month_num - 1) + str(row_number)  # Construct column key (e.g., "E11", "F12")

                if( json_data.get(month_column) is not None):
                    transformed_rows.append({
                        'file_data_id': file_id,
                        'jahr': 2024,
                        'monat': month_num,
                        'key_data': key,
                        'value_data': json_data.get(month_column),  # Extract value from JSON
                    })

    logger.info(f"Transformation completed. Total rows: {len(transformed_rows)}")
    logger.info("MM_Wechsel transformation completed successfully.")
    return transformed_rows   

@op
def write_to_mm_wechsel_table(transformed_data, cur):
    logger = get_dagster_logger()
    try:
        for row in transformed_data:
            try:
                cur.execute("""
                    INSERT INTO vw.mm_wechsel (file_data_id, jähr, monat, key_data, value_data)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    convert_to_float(row.get('file_data_id')),
                    row.get('jähr'),
                    str(row.get('monat')),  # Ensure month is stored as VARCHAR
                    row.get('key_data'),
                    convert_to_float(row.get('value_data')),
                ))
            except psycopg2.Error as e:
                logger.error(f"Error inserting row: {row}, Error: {e}")
                raise
    except Exception as e:
        logger.error(f"Error writing to database: {e}")
        raise
    finally:
        logger.info("Database transaction completed.")

def convert_to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None  # Return None if conversion fails

# Helper functions (you'll need to implement these based on your data)
def get_month_name(month_number):
    month_names = [
        "Jänner",
        "Februar",
        "März",
        "April",
        "Mai",
        "Juni",
        "Juli",
        "August",
        "September",
        "Oktober",
        "November",
        "Dezember",
    ]
    return month_names[month_number - 1] if 1 <= month_number <= 12 else None

def get_month_column(month_number):
    # Map month number to column identifier (e.g., "C11", "D11", etc.)
    # Adjust these based on your actual column identifiers
    month_columns = {
        1: "C11",
        2: "D11",
        3: "E11",
        4: "F11",
        5: "G11",
        6: "H11",
        7: "I11",
        8: "J11",
        9: "K11",
        10: "L11",
        11: "M11",
        12: "N11",
    }
    return month_columns.get(month_number)

def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        dbname="ecdwh", user="bruno", password="bruno", host="psqldb", port="5432"
    )
    
@op
def start_file_processing(context, file_info: List[dict]):
    context.log.info(f"Starting the Network Operator Job with file_info: {file_info}")
    logger = get_dagster_logger()
    
    # Process each file one by one with transactions
    for file_entry in file_info:
        file_id = file_entry["file_id"]
        sheet_names = file_entry["sheet_names"]
        file_name = file_entry["file_name"]
        
        logger.info(f"Processing file_id: {file_id} with sheets: {sheet_names} with file_name: {file_name}")
        
        try:
            # Start transaction
            conn = get_db_connection()
            cur = conn.cursor()
            conn.autocommit = False
            
            cur.execute( """ SELECT energy FROM File WHERE id = %s """, (file_id,) )  # Note the comma to make it a tuple
            energy_type = cur.fetchone()
            if energy_type is not None:
                # Handle both cases - whether it returns a tuple or directly the value
                energy_type_value = energy_type[0] if isinstance(energy_type, (tuple, list)) else energy_type
                logger.info(f"energy_type data: {type(energy_type_value)} - Value: {energy_type_value}")
            else:
                logger.info(f"No energy_type found for file_id: {file_id}")
             
            if "MM_Wechsel" in sheet_names:
                logger.info(f"Processing MM_Wechsel data for file_id: {file_id}")
                mm_wechsel_data = process_mm_wechsel_data(file_id, cur)
                transformed_mm_wechsel_data = transform_mm_wechsel_data(mm_wechsel_data, energy_type_value, file_name)
                write_to_mm_wechsel_table(transformed_mm_wechsel_data, cur) 
             
            if "MM_Bil" in sheet_names:
                logger.info(f"Processing MM_Bil data for file_id: {file_id}")
                mm_bil_data = process_mm_bil_data(file_id, cur)
                transformed_mm_bil_data = transform_mm_bil_data(mm_bil_data, energy_type_value)
                write_to_mm_bil_table(transformed_mm_bil_data, cur)

            if "Mengen für NV und Blindstrom" in sheet_names:
                logger.info(f"Processing Mengene_Blindstrom data for file_id: {file_id}")
                mengene_data = process_mengene_ev_blindstrom(file_id, cur)
                transform_mengene_data = extract_and_transform_mengene_blindstrom_data(mengene_data)
                write_data_to_mengene_blindstrom_table(transform_mengene_data, cur)    
            
            # Update file status to processed
            cur.execute("UPDATE File SET file_events = 'Success' WHERE id = %s", (file_id,))
            
            # Commit the transaction
            conn.commit()
            logger.info(f"Transaction for file_id {file_id} committed successfully")
            
        except Exception as e:
            # Rollback in case of error
            try:
                conn.rollback()
                # Update file status to failed
                conn.autocommit = True
                cur.execute("UPDATE File SET file_events = 'Failed' WHERE id = %s", (file_id,))
            except:
                pass
            
            logger.error(f"Error processing file_id {file_id}: {str(e)}")
            
        finally:
            # Close connection
            try:
                conn.autocommit = True
                cur.close()
                conn.close()
            except:
                pass
    
    return file_info      

@job
def process_file_job():
    logger = get_dagster_logger()
    logger.info("Starting file processing Job...")
    start_file_processing()