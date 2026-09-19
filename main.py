import os
import json
import requests
import datetime
from google.cloud import storage

def get_paypal_token(client_id, client_secret):
    url = "https://api-m.sandbox.paypal.com/v1/oauth2/token"
    data = {"grant_type": "client_credentials"}
    response = requests.post(url, auth=(client_id, client_secret), data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def extract_transactions(token):
    url = "https://api-m.sandbox.paypal.com/v1/reporting/transactions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # AUTOMATION: Automatically calculate exactly yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    start_date = yesterday.strftime("%Y-%m-%dT00:00:00-0700")
    end_date = yesterday.strftime("%Y-%m-%dT23:59:59-0700")
    
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

# This is the main function Google Cloud will run
def run_extraction(request):
    # Grab credentials securely from GCP Environment Variables
    CLIENT_ID = os.environ.get("PAYPAL_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("PAYPAL_CLIENT_SECRET")
    
    token = get_paypal_token(CLIENT_ID, CLIENT_SECRET)
    raw_data = extract_transactions(token)
    
    # Cloud Functions require saving temporary files to the /tmp folder
    local_filename = "/tmp/raw_paypal_data.json"
    with open(local_filename, "w") as file:
        json.dump(raw_data, file, indent=4)
        
    TARGET_BUCKET = "paypal_pipeline" 
    
    # AUTOMATION: Add yesterday's date to the filename so they don't overwrite!
    yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    destination_name = f"raw_data/paypal_transactions_{yesterday_str}.json"
    
    upload_to_gcs(TARGET_BUCKET, local_filename, destination_name)
    
    return "Extraction successful", 200