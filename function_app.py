import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import json
import os

# Initialize the Function App using the V2 programming model
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="DataProcessorApi")
def DataProcessorApi(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger function to read nutritional data from Azure Blob Storage,
    process it using pandas, and return the average macronutrients as JSON.
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

        # --- 3. Calculate Averages (Existing Logic) ---
        avg_macros = df.groupby('Diet_type')[['Protein(g)', 'Carbs(g)', 'Fat(g)']].mean()

        # --- 4. Calculate Recipe Counts (NEW LOGIC) ---
        # Calculate the count of recipes for each Diet_type
        diet_counts = df['Diet_type'].value_counts().reset_index()
        diet_counts.columns = ['Diet_type', 'Recipe_Count']

        # --- 5. Merge DataFrames ---
        # Merge the average macros with the recipe counts on 'Diet_type'
        final_results = pd.merge(avg_macros.reset_index(), diet_counts, on='Diet_type')

        # --- 6. Return Results ---
        # Convert the final DataFrame to a list of dictionaries for JSON output
        results_json = final_results.to_dict(orient='records')

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
    
# az login
# func azure functionapp publish data-processor-api