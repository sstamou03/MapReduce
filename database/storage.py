import io
import uuid
import os
import json
import hashlib
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

def delete_job_files(job_id: str):
    """
    Deletes all files in MinIO related to a specific job.
    Called when a job is aborted or a user is deleted.
    """
    prefix = f"job-{job_id}/"
    buckets_to_clean = ["mapreduce-intermediates", "mapreduce-inputs", "mapreduce-outputs"]

    for bucket in buckets_to_clean:
        try:
            if minio_client.bucket_exists(bucket):
                objects_to_delete = minio_client.list_objects(bucket, prefix=prefix, recursive=True)
                for obj in objects_to_delete:
                    minio_client.remove_object(bucket, obj.object_name)
        except S3Error as e:
            print(f"[-] Error cleaning bucket {bucket} for job {job_id}: {e}")



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


#shuffle
def shuffle_intermediate_results(job_id: str, intermediate_refs: list, num_reducers: int) -> list:
    """
    Downloads all intermediate JSON logs from MinIO, parses them, 
    partitions them into `num_reducers` chunks based on a hash of the key,
    and uploads each partition back to MinIO.
    Returns a list of MinIO references for the Reducers to use.
    """
    # Create an array of lists, one for each reducer partition
    partitions = [[] for _ in range(num_reducers)]
    
    for ref in intermediate_refs:
        try:
            raw_bytes = get_data_from_ref(ref)
            data = json.loads(raw_bytes.decode("utf-8"))
            
            # Ensure data is a list of [key, value] pairs
            if not isinstance(data, list):
                data = [data]
                
            for item in data:
                # We expect item to be like [key, value]. We hash the key.
                if isinstance(item, list) and len(item) >= 1:
                    key_str = str(item[0])
                else:
                    # Fallback if the mapper didn't return a standard format
                    key_str = str(item)
                    
                # Use MD5 to get a consistent hash, then modulo num_reducers
                hash_val = int(hashlib.md5(key_str.encode('utf-8')).hexdigest(), 16)
                partition_idx = hash_val % num_reducers
                partitions[partition_idx].append(item)
                
        except Exception as e:
            print(f"Error shuffling reference {ref}: {e}")
            
    # Upload the partitioned results
    bucket_name = "mapreduce-intermediates"
    partition_refs = []
    
    for i, partition_data in enumerate(partitions):
        # Even if a partition is empty, we create an empty list so the reducer doesn't fail
        combined_bytes = json.dumps(partition_data).encode("utf-8")
        object_name = f"job-{job_id}/shuffled_output_{i}.json"
        ref = upload_data_bytes(bucket_name, object_name, combined_bytes)
        partition_refs.append(ref)
    
    return partition_refs
