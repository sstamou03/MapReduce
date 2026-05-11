import os
import sys
import uuid
import json
import tempfile
import importlib.util
import requests
from unittest.mock import MagicMock
sys.modules['psycopg2'] = MagicMock()
from database.storage import download_file, get_data_from_ref, upload_intermediate_result, upload_data_bytes, upload_final_result

# The Manager's base URL — configurable via env var, defaults to K8s service name
MANAGER_URL = os.getenv("MANAGER_URL", "http://manager-service:8000")


"""
Worker Service for the MapReduce platform.

Its primary responsibilities are:
* Identifying its assigned task via OS Environment Variables.
* Connecting to MinIO to download raw data chunks and user-provided Python scripts.
* Dynamically executing the user's 'map' or 'reduce' functions.
* Uploading intermediate and final results back to MinIO.
* Reporting its success or failure status back to the Manager Service via HTTP.
"""



def load_user_module(code_path):
    """Dynamically loads a Python file from disk and returns it as a module."""
    spec = importlib.util.spec_from_file_location("user_code", code_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def report_status(task_id, status, output_ref=None):
    """Sends an HTTP POST to Manager to report task status."""
    payload = {
        "task_id": task_id,
        "status": status,
        "worker_pod_id": os.getenv("HOSTNAME", "local-worker"),
    }
    if output_ref:
        payload["output_partition_ref"] = output_ref

    try:
        resp = requests.post(f"{MANAGER_URL}/manager/tasks/status", json=payload)
        resp.raise_for_status()
        print(f"[*] Reported status '{status}' to Manager.")
    except Exception as e:
        print(f"[-] Could not reach Manager to report status: {e}")


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

    # Tell the Manager we are starting
    report_status(task_id, "RUNNING")

    # Execute the user's code
    try:
        user_module = load_user_module(code_path)

        if task_type.upper() == "MAP":
            result = user_module.map(text_content)
        elif task_type.upper() == "REDUCE":
            result = user_module.reduce(text_content)
        else:
            print(f"[-] Unknown TASK_TYPE: {task_type}")
            sys.exit(1)

        print(f"[*] Execution finished successfully!")

        # Upload results back to MinIO
        result_bytes = json.dumps(result).encode("utf-8")
        if task_type.upper() == "MAP":
            output_ref = upload_intermediate_result(job_id, task_id, result_bytes)
        else:
            output_ref = upload_final_result(job_id, task_id, result_bytes)
        print(f"[*] Result uploaded to {output_ref}")

        # Tell the Manager we are done
        report_status(task_id, "COMPLETED", output_ref=output_ref)

    except Exception as e:
        print(f"[-] Execution failed: {e}")
        report_status(task_id, "FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()
