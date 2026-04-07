import os
import sys
import uuid
import tempfile
from unittest.mock import MagicMock
sys.modules['psycopg2'] = MagicMock()

from database.storage import download_file, get_data_from_ref


"""
Worker Service for the MapReduce platform.

Its primary responsibilities are:
* Identifying its assigned task via OS Environment Variables.
* Connecting to MinIO to download raw data chunks and user-provided Python scripts.
* Dynamically executing the user's 'map' or 'reduce' functions.
* Uploading intermediate and final results back to MinIO.
* Reporting its success or failure status back to the Manager Service via HTTP.
"""



def main():

    print("=== Worker Node Starting ===")
    
    task_id = os.getenv("TASK_ID")
    job_id = os.getenv("JOB_ID")
    task_type = os.getenv("TASK_TYPE") # "MAP" or "REDUCE"
    input_ref = os.getenv("INPUT_REF")
    code_ref = os.getenv("CODE_REF") # The python script location in MinIO
    
    if not all([task_id, job_id, task_type, input_ref, code_ref]):
        print("[!]Error: Missing required environment variables. The Manager didn't configure me correctly!")
        sys.exit(1)
        
    print(f"[*] Task ID: {task_id}")
    print(f"[*] Job ID: {job_id}")
    print(f"[*] I am a {task_type} worker!")
    print(f"[*] My assigned data is at: {input_ref}")
    print(f"[*] My assigned code is at: {code_ref}")
    
    #Connect to MinIO
    try:

        # get_data_from_ref is the function from database/storage.py, it handles the connection and returns the raw bytes
        chunk_data = get_data_from_ref(input_ref)
        
        # We decode the bytes into a string so we can read it
        text_content = chunk_data.decode("utf-8")
        
        print("[*] Successfully connected to MinIO and downloaded the data chunk!")
        print(f"[*] Data length: {len(text_content)} characters.")
        

        # download the Python script
        bucket, obj_name = code_ref.split("/", 1)
        code_path = os.path.join(tempfile.gettempdir(), f"{task_id}_script.py")
        download_file(bucket, obj_name, code_path)
        
        print(f"[*] Successfully downloaded the Python script to {code_path}!")
        

    except Exception as e:
        print(f"[-] Failed to reach MinIO: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
