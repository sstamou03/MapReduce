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


#input partitioning

def split_and_upload_input(job_id: str, input_ref: str, num_mappers: int) -> list:
    """
    Splits a large input file into smaller chunks (one per mapper) and uploads
    each chunk to MinIO as a separate object.

    This is the core function that enables parallel MapReduce processing.
    The Manager service calls this before creating mapper tasks.

    Args:
        job_id: The UUID of the job.
        input_ref: MinIO reference to the full input file (e.g., "mapreduce-inputs/job-xyz/input_data").
        num_mappers: How many mappers (chunks) to split the input into.

    Returns:
        A list of MinIO references, one per chunk. Example:
        ["mapreduce-inputs/job-xyz/partition_0",
         "mapreduce-inputs/job-xyz/partition_1",
         "mapreduce-inputs/job-xyz/partition_2"]
    """
    # Step 1: Download the full input file from MinIO
    raw_data = get_data_from_ref(input_ref)
    all_lines = raw_data.decode("utf-8").splitlines(keepends=True)

    # Step 2: Calculate how many lines each mapper gets
    total_lines = len(all_lines)
    lines_per_chunk = total_lines // num_mappers
    remainder = total_lines % num_mappers

    # Step 3: Split into chunks and upload each one
    bucket_name = "mapreduce-inputs"
    partition_refs = []
    start = 0

    for i in range(num_mappers):
        # Distribute remainder lines across the first chunks (1 extra line each)
        chunk_size = lines_per_chunk + (1 if i < remainder else 0)
        chunk_lines = all_lines[start:start + chunk_size]
        start += chunk_size

        # Convert chunk back to bytes and upload to MinIO
        chunk_data = "".join(chunk_lines).encode("utf-8")
        object_name = f"job-{job_id}/partition_{i}"
        ref = upload_data_bytes(bucket_name, object_name, chunk_data)
        partition_refs.append(ref)

    return partition_refs