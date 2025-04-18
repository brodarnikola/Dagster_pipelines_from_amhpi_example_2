#from dagster import job, op, get_dagster_logger, Definitions 
from dagster import (
    job, 
    op, 
    get_dagster_logger, 
    Definitions, 
)
import pandas as pd 
#from definitions import get_db_connection, calculate_sha256_hash
import psycopg2 # For PostgreSQL interaction 
from dotenv import load_dotenv 
import os
from datetime import datetime 
from typing import Union, List, Dict
from io import BytesIO

load_dotenv(dotenv_path="configs.env")  # Create this file

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')  # Simplified endpoint URL
DB_PASSWORD = os.getenv('DB_PASSWORD')  # Simplified endpoint URL
        
@op
def process_mm_bil_data(file_id: int, cur): #(file_ids: List[int]):
    logger = get_dagster_logger()
    data = []
    try:
        # conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        # cur = conn.cursor()

        cur.execute("SELECT json_data, file_id FROM File_data WHERE sheet_name = %s and file_id = %s", ("MM_Bil",file_id))
        data = cur.fetchall()
 
        # placeholders = ','.join(['%s'] * len(file_ids))
        # query = f"SELECT json_data, file_id FROM File_data WHERE sheet_name = %s AND file_id IN ({placeholders})"
        # params = ["MM_Bil"] + file_ids
        # cur.execute(query, params)
        # data = cur.fetchall()
        
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

    finally: 
        logger.info("Database data fetched.")
        return data    

@op
def transform_mm_bil_data(data: List[tuple], energy_type): # -> List[Dict]:  
    logger = get_dagster_logger()
    logger.info("Starting data transformation...")

    new_rows = []
    year = "2024"  # Fixed for now

    # Define the keys (previously column names) that will become `key_data` in the new table
    if( energy_type == "Gas"):
        keys_to_extract = {
            "2022": 11,
            "2023": 12,
            "2024": 13,
            "2025": 14,
            "2026": 29,
            "2027": 30,
            "2028": 31,
            "2029": 32,
            #"2030": 33,
            "2031": 34,
            "2032": 35,
            # "netz_abgabe_haushalt": 31,
            # "netz_abgabe_sozial": 32,
            # "netz_abgabe_leistungsgemessen": 34,
        }
    else:
        keys_to_extract = {
            "1633": 11,
            "1634": 12,
            "1635": 13,
            "1636": 14,
            "1637": 15,
            "1638": 16,
            "1639": 17,
            "1640": 18,
            "1641": 19,
            "1642": 20,
            "1643": 21,
            "1644": 22,
            "1645": 23,
            "1646": 24,
            "1647": 25,
            "1648": 26,
            "1649": 27,
            "1650": 28, 
            "1651": 29    
        }     

    # Iterate through each row of data
    for json_data_tuple in data:
        json_data = json_data_tuple[0]  # Extract the actual JSON data
        file_id = json_data_tuple[1]  # Extract the file_id
        
        if( energy_type == "Gas"):
            start_month = 'B'
        else:
            start_month = 'C'

        # Loop through each metric (key) and create a row for each month
        for key, row_number in keys_to_extract.items():
            for month_num in range(1, 13):
                month_column = chr(ord(start_month) + month_num) + str(row_number)  # Construct column key (e.g., "C11", "D12")

                new_row = {
                    'file_data_id': file_id,
                    'jähr': year,
                    'monat': month_num,
                    'key_data': key,
                    'value_data': json_data.get(month_column, None),  # Extract value from the JSON
                }

                new_rows.append(new_row)

    logger.info(f"Transformation completed. Total rows: {len(new_rows)}")
    return new_rows

def convert_to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None  # Return None if conversion fails


# Helper functions (you'll need to implement these based on your data)
def get_month_name(month_number):
    month_names = ["Jänner", "Februar", "März", "April", "Mai", "Juni", 
                   "Juli", "August", "September", "Oktober", "November", "Dezember"]
    return month_names[month_number - 1] if 1 <= month_number <= 12 else None

def get_month_column(month_number):
    # Map month number to column identifier (e.g., "C11", "D11", etc.)
    # Adjust these based on your actual column identifiers
    month_columns = {
        1: "C11", 2: "D11", 3: "E11", 4: "F11", 5: "G11", 6: "H11",
        7: "I11", 8: "J11", 9: "K11", 10: "L11", 11: "M11", 12: "N11"
    }
    return month_columns.get(month_number)            

@op
def write_to_mm_bil_table(transformed_data, cur):
    logger = get_dagster_logger()
    try:
        for row in transformed_data:
            try:
                cur.execute("""
                    INSERT INTO vw.mm_bil (file_data_id, jähr, monat, key_data, value_data)
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
