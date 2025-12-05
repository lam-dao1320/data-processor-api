import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import os
import json
import logging

# Initialize the Function App using the V2 programming model
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="DataProcessorApi")

def DataProcessorApi(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger function to read nutritional data from Azure Blob Storage, process it using pandas, and return the average macronutrients as JSON.
    """
    try:
        # --- 1. Get Connection String (Replaces hardcoded Azurite string) ---
        # NOTE: This retrieves the connection string from Azure Function App Settings.
        connect_str = os.environ.get('DATA_STORAGE_CONNECTION')
        if not connect_str:
            return func.HttpResponse(
                "DATA_STORAGE_CONNECTION environment variable not found.",
                status_code=500
            )

        # --- 2. Blob Storage Setup ---
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Define the container and blob names (as used in process_nutrition_data.py)
        container_name = 'datasets'
        blob_name = 'All_Diets.csv'

        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        # --- 3. Download and Load Data (Core logic from process_nutrition_data.py) ---
        # Download blob content to bytes
        stream = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(stream))
        
        logging.info(f"Raw data loaded. Shape: {df.shape}")

        # --- 2. Perform Data Cleaning (Business Logic) ---
        
        df_cleaned = df.dropna(subset=['Diet_type', 'Cuisine_type'])
        numeric_cols = ['Protein(g)', 'Carbs(g)', 'Fat(g)']
        df_cleaned[numeric_cols] = df_cleaned[numeric_cols].fillna(df_cleaned[numeric_cols].mean())
        df_cleaned[numeric_cols] = df_cleaned[numeric_cols].clip(lower=0)
        
        logging.info(f"Successfully uploaded cleaned. New Shape: {df_cleaned.shape}")
        

        # --- 3. Return Cleaned Data ---
        # Convert the final DataFrame to a list of dictionaries for JSON output
        results_json = df_cleaned.to_dict(orient='records')

        return func.HttpResponse(
            # Return the processed data as a JSON string
            json.dumps(results_json),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        # Log the error and return a 500 status code
        print(f"An unexpected error occurred: {e}")
        return func.HttpResponse(
            f"Error processing data: {str(e)}",
            status_code=500
        )