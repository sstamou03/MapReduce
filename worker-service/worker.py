import os
import sys
import uuid
from unittest.mock import MagicMock
sys.modules['psycopg2'] = MagicMock()

from database.storage import download_file, get_data_from_ref

def main():
    print("=== Worker Node Starting ===")
    
    task_id = os.getenv("TASK_ID")
    job_id = os.getenv("JOB_ID")
    task_type = os.getenv("TASK_TYPE") # "MAP" or "REDUCE"
    input_ref = os.getenv("INPUT_REF")
    
    if not all([task_id, job_id, task_type, input_ref]):
        print("[!]Error: Missing required environment variables. The Manager didn't configure me correctly!")
        sys.exit(1)
        
    print(f"[*] Task ID: {task_id}")
    print(f"[*] Job ID: {job_id}")
    print(f"[*] I am a {task_type} worker!")
    print(f"[*] My assigned data is at: {input_ref}")
    
    #Connect to MinIO and download our data
    try:

        # get_data_from_ref is the function from database/storage.py, it handles the connection and returns the raw bytes
        chunk_data = get_data_from_ref(input_ref)
        
        # We decode the bytes into a string so we can read it
        text_content = chunk_data.decode("utf-8")
        
        print("[*] Successfully connected to MinIO and downloaded the data chunk!")
        print(f"[*] Data length: {len(text_content)} characters.")
        

    except Exception as e:
        print(f"[-] Failed to reach MinIO: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
