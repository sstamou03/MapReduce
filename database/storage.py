import io
import uuid
import os
from minio import Minio
from minio.error import S3Error


"""
MinIO Storage utility for the MapReduce platform.
Handles connecting to the local MinIO container and provides functions
to upload input data, mapper/reducer code, and intermediate results.
"""


# Initialize MinIO client.
# Assuming standard MinIO properties based on the docker-compose setup.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "12345678")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def ensure_bucket_exists(bucket_name: str):
    """Ensure the specified MinIO bucket exists, creating it if necessary."""
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
    except S3Error as e:
        print(f"Error checking or creating bucket {bucket_name}: {e}")

def upload_file(bucket_name: str, object_name: str, file_path: str) -> str:
    """
    Upload a local file to the specified MinIO bucket.
    """
    ensure_bucket_exists(bucket_name)
    minio_client.fput_object(bucket_name, object_name, file_path)
    return f"{bucket_name}/{object_name}"

def upload_data_bytes(bucket_name: str, object_name: str, data: bytes) -> str:
    """
    Upload raw bytes (e.g., from an in-memory buffer) to the specified MinIO bucket.
    """
    ensure_bucket_exists(bucket_name)
    data_stream = io.BytesIO(data)
    minio_client.put_object(bucket_name, object_name, data_stream, length=len(data))
    return f"{bucket_name}/{object_name}"

def upload_input_data(job_id: str, file_path: str) -> str:
    """
    Uploads the input dataset for a specific job.
    """
    bucket_name = "mapreduce-inputs"
    object_name = f"job-{job_id}/input_data"
    return upload_file(bucket_name, object_name, file_path)

def upload_code(job_id: str, role: str, file_path: str) -> str:
    """
    Uploads user code (mapper or reducer).
    role: e.g. 'mapper' or 'reducer'
    """
    bucket_name = "mapreduce-code"
    object_name = f"job-{job_id}/{role}.py"
    return upload_file(bucket_name, object_name, file_path)

def upload_intermediate_result(job_id: str, task_id: str, data: bytes) -> str:
    """
    Uploads an intermediate result (e.g., from a Map task) to MinIO.
    """
    bucket_name = "mapreduce-intermediates"
    object_name = f"job-{job_id}/task-{task_id}_output"
    return upload_data_bytes(bucket_name, object_name, data)

def download_file(bucket_name: str, object_name: str, file_path: str):
    """
    Download an object from MinIO to a local file.
    """
    minio_client.fget_object(bucket_name, object_name, file_path)

def get_data_bytes(bucket_name: str, object_name: str) -> bytes:
    """
    Fetch an object from MinIO and return its content as bytes.
    Useful for reading map or reduce output directly into memory.
    """
    response = None
    try:
        response = minio_client.get_object(bucket_name, object_name)
        return response.read()
    finally:
        if response:
            response.close()
            response.release_conn()

def get_data_from_ref(ref: str) -> bytes:
    """
    Fetch data from MinIO using a reference string formatted as 'bucket_name/object_name'.
    Commonly used by Mappers or Reducers to read their assigned input_partition_ref.
    """
    parts = ref.split('/', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid storage reference format: {ref}. Expected 'bucket_name/object_name'.")
    bucket_name, object_name = parts
    return get_data_bytes(bucket_name, object_name)
