#from dagster import job, op, get_dagster_logger, Definitions 
from dagster import (
    job, 
    op, 
    get_dagster_logger,  
)
import pandas as pd 
#from definitions import get_db_connection, calculate_sha256_hash
import psycopg2 # For PostgreSQL interaction 
from dotenv import load_dotenv 
import os
from datetime import datetime  
from io import BytesIO

from typing import Dict, Optional, List, Tuple

load_dotenv(dotenv_path="configs.env")  # Create this file

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')  # Simplified endpoint URL
DB_PASSWORD = os.getenv('DB_PASSWORD')  # Simplified endpoint URL
        
@op
def process_mengene_ev_blindstrom(file_id: int, cur):#(file_ids: List[int]):
    logger = get_dagster_logger()
    data = []
    try:
        
        logger.info("Starting data transformation...") 
        
        cur.execute("SELECT id, json_data, file_id FROM File_data WHERE sheet_name = %s and file_id = %s", ("Mengen für NV und Blindstrom", file_id))
        data = cur.fetchall()

        # Construct the IN clause for file_ids
        # placeholders = ','.join(['%s'] * len(file_ids))
        # #query = f"SELECT id, json_data, file_id FROM File_data WHERE sheet_name = %s AND file_id IN ({placeholders})"
        # query = f"SELECT json_data, file_id FROM File_data WHERE sheet_name = %s AND file_id IN ({placeholders})"
        # params = ["Mengen für NV und Blindstrom"] + file_ids

        # cur.execute(query, params)
        # data = cur.fetchall()
 
    except Exception as e: 
        logger.error(f"Database error: {e}")
        raise
    finally:
        logger.info("Database data fetched.")
        return data    
  

@op
def extract_and_transform_mengene_blindstrom_data(data: List[Tuple]) -> List[Dict]:
    logger = get_dagster_logger()
    extracted_data_list: List[Dict] = []

    if not data:
        logger.warning("No data received for transformation.")
        return extracted_data_list

    for id, json_data, file_id in data:
        for prefix in ["B.2.8", "B.2.9"]:
            base_key = prefix + "."
            for i in range(1, 8):
                extracted_data: Dict[str, Optional[str]] = {
                    "ckey": base_key + str(i),
                    "ebene": str(i),
                    "ebene_titel": json_data.get(f"D{i+2}", None) if prefix == "B.2.8" else json_data.get(f"D{i+12}", None),
                    "endverbraucher_wv": json_data.get(f"E{i+2}", None) if prefix == "B.2.8" else json_data.get(f"H{i+12}", None),
                    "einspeiser": json_data.get(f"F{i+2}", None) if prefix == "B.2.8" else None,
                    "pumpspeicher": json_data.get(f"G{i+2}", None) if prefix == "B.2.8" else None,
                    "kommentar": None,
                    "file_id": file_id
                }
                extracted_data_list.append(extracted_data)
    logger.info(f"Extracted and transformed {len(extracted_data_list)} rows.")
    return extracted_data_list

@op
def write_data_to_mengene_blindstrom_table( transformed_data: List[Dict], cur): # Added type hint for transformed_data
    logger = get_dagster_logger() 
    try:

        for data in transformed_data:
            file_id = data.pop("file_id", None)  # Extract file_id, remove from data
            if file_id is None:
                logger.error("Missing file_id in data")
                continue

            try:
                cur.execute(
                    """
                    INSERT INTO tarife.mengen_nv_blindstrom 
                    (ckey, ebene, ebene_titel, endverbraucher_wv, einspeiser, pumpspeicher, kommentar, file_id) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        data["ckey"],
                        data["ebene"],
                        data["ebene_titel"],
                        data["endverbraucher_wv"],
                        data["einspeiser"],
                        data["pumpspeicher"],
                        data["kommentar"],
                        file_id,
                    ),
                ) 
            except psycopg2.Error as e: 
                logger.error(f"Error inserting data: {data}, Error: {e}")

        logger.info("Finished inserting data.")
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        logger.info("Database data saved into transaction.")