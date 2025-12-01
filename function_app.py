import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import os
import logging

# --- Configuration Constants ---
# NOTE: The connection string is read from Application Settings: DATA_STORAGE_CONNECTION
CLEAN_CONTAINER_NAME = 'cleaned-datasets' 
# CRITICAL FIX: Use a static name to ensure the file is overwritten
CLEAN_BLOB_NAME = 'All_Diets_Cleaned.csv'

# Initialize the Function App using the V2 programming model
app = func.FunctionApp()

# --- BLOB TRIGGER FUNCTION ---
@app.blob_trigger(
    arg_name="myblob", 
    path="datasets/All_Diets.csv", 
    connection="DATA_STORAGE_CONNECTION" # Uses the secret key to monitor the container
)
def ProcessNewDietData(myblob: func.InputStream):
    """
    Function triggered automatically when the All_Diets.csv file is created or updated in the 'datasets' container.
    It reads the file, performs cleaning, and saves the cleaned data to a NEW FILE NAME in a different container, overwriting the old clean data.
    """
    logging.info(f"Python Blob trigger function started processing blob:\n"
                 f"  Name: {myblob.name}\n"
                 f"  URI: {myblob.uri}\n"
                 f"  Length: {myblob.length} bytes")

    try:
        # --- 1. Read Raw Data from Trigger Stream ---
        stream = io.BytesIO(myblob.read())
        df = pd.read_csv(stream)
        
        logging.info(f"Raw data loaded. Shape: {df.shape}")

        # --- 2. Perform Data Cleaning (Business Logic) ---
        
        df_cleaned = df.dropna(subset=['Diet_type', 'Cuisine_type'])
        numeric_cols = ['Protein(g)', 'Carbs(g)', 'Fat(g)']
        df_cleaned[numeric_cols] = df_cleaned[numeric_cols].fillna(df_cleaned[numeric_cols].mean())
        df_cleaned[numeric_cols] = df_cleaned[numeric_cols].clip(lower=0)
        
        logging.info(f"Data cleaned. New Shape: {df_cleaned.shape}")

        # --- 3. Write Cleaned Data to New Blob Storage (Overwrites Old File) ---
        
        connect_str = os.environ.get("DATA_STORAGE_CONNECTION")
        if not connect_str:
            logging.error("DATA_STORAGE_CONNECTION environment variable not found.")
            return
            
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        
        container_client = blob_service_client.get_container_client(CLEAN_CONTAINER_NAME)
        container_client.create_container(fail_on_exist=False)
        
        # --- CRITICAL CHANGE: Use the static output name ---
        output_blob_name = CLEAN_BLOB_NAME 
        
        # Convert DataFrame back to CSV format in memory
        output_stream = io.StringIO()
        df_cleaned.to_csv(output_stream, index=False)
        
        # Upload the stream, using overwrite=True to replace the old clean file
        blob_client = container_client.get_blob_client(output_blob_name)
        blob_client.upload_blob(output_stream.getvalue(), overwrite=True)
        
        logging.info(f"Successfully uploaded cleaned data, overwriting old file at: {CLEAN_CONTAINER_NAME}/{output_blob_name}")
        
    except Exception as e:
        logging.error(f"Error processing blob data: {e}")