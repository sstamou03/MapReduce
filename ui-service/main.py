from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt
from sqlalchemy.orm import Session
from typing import List
import uuid
import sys
import asyncio

import httpx
from contextlib import asynccontextmanager

from database import schemas, crud, storage
from database.db import get_db

# we configure logging to see it in terminal
import logging
import colorlog
import time

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    fmt="%(log_color)s[%(levelname)-8s]%(reset)s [%(asctime)s] : %(message)s",
    datefmt="%H:%M:%S",
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'green',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'red,bg_white',
    }
))

logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

#we store here all the keys from keycloak, to prevent a token expiring at the moment keycloak rotates keys
KEYCLOAK_PUBLIC_KEY : str = ""

# we will grab the public key from keycloak at startup, not hardcode it like before
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP: Fetch Key from Keycloak ---
    global KEYCLOAK_PUBLIC_KEY
    realm_url = "http://mapreduce-keycloak:8080/realms/MapReduce-Realm"
    
    max_retries = 10
    retry_delay = 4  # seconds
    
    async with httpx.AsyncClient() as client:
        for i in range(max_retries):
            try:
                logger.info(f"Attempting to fetch Keycloak public key (Attempt {i+1}/{max_retries})...")
                response = await client.get(realm_url, timeout=10.0)
                response.raise_for_status()
                data = response.json()
                
                raw_key = data.get("public_key")
                KEYCLOAK_PUBLIC_KEY = (
                    f"-----BEGIN PUBLIC KEY-----\n"
                    f"{raw_key}\n"
                    f"-----END PUBLIC KEY-----"
                )
                logger.info("Successfully fetched Keycloak public key dynamically.")
                break  # Success! Exit the loop
                
            except Exception as e:
                if i < max_retries - 1:
                    logger.warning(f"Keycloak not ready yet ({e}). Retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.critical(f"Failed to fetch Keycloak key after {max_retries} attempts: {e}")
                    sys.exit(1)
    yield

app = FastAPI(lifespan=lifespan)

# now this tells fastAPI where to search for a token
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="http://localhost:8080/realms/MapReduce-Realm/protocol/openid-connect/token"
)

"""====================================================== 
                        AUTH & RBAC
 ====================================================== """

#this is to extract both the username and role in a dictionary
async def get_current_user_id(token: str = Depends(oauth2_scheme)):
    """
    decode the JWT and extracts the user's unique ID.
    """
    try:
        payload = jwt.decode(
            token, 
            KEYCLOAK_PUBLIC_KEY, #used to verify token that was signed with Keycloak's private key
            algorithms=["RS256"],
            options={"verify_aud": False}
        )        
        user_id: str = payload.get("sub")
        resource_access = payload.get("resource_access", {})
        ui_service_access = resource_access.get("ui-service", {})
        roles = ui_service_access.get("roles", [])

        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
            
        return {"user_id": user_id, "roles": roles}
    
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.JWTError as e:
        # this catches invalid signatures or tampered tokens
        raise HTTPException(status_code=401, detail=f"Token validation failed: {str(e)}")

# this is to extract only the user id
async def get_verified_user_id(user_data: dict = Depends(get_current_user_id)) -> str:
    return user_data["user_id"]

# this is more specific, for when we want only admin users
async def get_admin_user(user_data: dict = Depends(get_current_user_id)) -> str:
    if "admin" not in user_data["roles"]:
        logger.warning(f"User {user_data['user_id']} tried to access admin endpoint")
        raise HTTPException(
            status_code=403,
            detail="Admin role required"
        )
        
    return user_data["user_id"]

def is_admin(user_data: dict) -> bool:
    """
    Check if the user is an admin.
    """
    return True if "admin" in user_data["roles"] else False


"""====================================================== 
                        ENDPOINTS
 ====================================================== """

### DONE FOR NOW
@app.post("/files/upload", response_model=schemas.FileUploadResponse, tags=["Files"])
async def upload_files(
    input_data: UploadFile = File(...), 
    mapper_code: UploadFile = File(...), 
    reducer_code: UploadFile = File(...),
    user_id: str = Depends(get_verified_user_id)
):
    '''
    1) upload files to minio -> submit input files and return pointers
    '''
    # we have no job creation yet, so for now generate a unique prefix for this set of files
    # this will be the job_id in the future, instead of a random string
    logger.info(f"User {user_id} requested to upload files for a job.")

    user_prefix = f"user-{user_id}"

    try:
        #read input in bytes
        input_bytes = await input_data.read()
        mapper_bytes = await mapper_code.read()
        reducer_bytes = await reducer_code.read()

        #now, upload them using functions in /database/storage.py
        input_ref = storage.upload_data_bytes("mapreduce-inputs", f"{user_prefix}/{input_data.filename}", input_bytes)
        mapper_ref = storage.upload_data_bytes("mapreduce-code", f"{user_prefix}/{mapper_code.filename}", mapper_bytes)
        reducer_ref = storage.upload_data_bytes("mapreduce-code", f"{user_prefix}/{reducer_code.filename}", reducer_bytes)

        return {
            "input_ref": input_ref,
            "mapper_ref": mapper_ref,
            "reducer_ref": reducer_ref
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during file upload: {str(e)}")
    

### (TODO) : SEND THE REQUEST TO MANAGER
@app.post("/jobs", response_model=schemas.JobResponse, tags=["Jobs"])
async def submit_job(
    job_in: schemas.JobCreate, 
    user_id: str = Depends(get_verified_user_id),
    db: Session = Depends(get_db)
):
    '''
    2) submit MapReduce job -> forward request to manager service
        - save job metadata to postgres
        - notify manager service
    '''

    # Validate that the references exist in MinIO
    for ref in [job_in.input_code_ref, job_in.mapper_code_ref, job_in.reducer_code_ref]:
        if not storage.check_ref_exists(ref):
            logger.error(f"User {user_id} tried to submit job with invalid MinIO ref: {ref}")
            raise HTTPException(status_code=400, detail=f"Invalid storage reference: '{ref}' does not exist in MinIO.Please re-upload your files or try again.")

    try:
        new_job = crud.create_job(
            db=db,
            user_id=user_id,
            input_code_ref=job_in.input_code_ref,
            mapper_code_ref=job_in.mapper_code_ref,
            reducer_code_ref=job_in.reducer_code_ref,
            num_mappers=job_in.num_mappers,
            num_reducers=job_in.num_reducers
        )
        
        logger.info(f"User {user_id} submitted a new job {new_job.job_id}.")

        # Forward the job to the Manager service so it can start orchestration
        try:
            async with httpx.AsyncClient() as client:
                manager_response = await client.post(
                    "http://manager-service:8000/manager/jobs",
                    json={"job_id": str(new_job.job_id)},
                    timeout=10.0
                )
                manager_response.raise_for_status()
                logger.info(f"Manager accepted job {new_job.job_id} for orchestration.")
        except Exception as mgr_err:
            logger.warning(f"Manager notification failed for job {new_job.job_id}: {mgr_err}. Job is saved but not yet orchestrated.")

        return new_job
    
    except Exception as e:
        logger.error(f"DATABASE ERROR: Failed to create job for {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Could not register job in database.")

### DONE FOR NOW
@app.get("/jobs", response_model=List[schemas.JobResponse], tags=["Jobs"])
async def get_user_jobs(user_id: str = Depends(get_verified_user_id), db: Session = Depends(get_db)):
    '''
    list all jobs for the authenticated user
    '''
    # get_current_user_id() --> dictionary with username and role.
    # get_verified_user_id() --> keeps only the user id
    # so this function now calls get_verified_user_id() to get the user id

    logger.info(f"User {user_id} requested to list their jobs.")
    return crud.get_jobs_for_user(db, user_id)

### DONE FOR NOW
@app.get("/jobs/{job_id}", response_model=schemas.JobDetailResponse, tags=["Jobs"])
async def get_job(
    job_id : uuid.UUID,
    user_data : dict = Depends(get_current_user_id),
    db: Session = Depends(get_db)
):

    logger.info(f"User {user_data.get('user_id')} requested details for job {job_id}.")

    user_id = user_data.get('user_id')
    job = crud.get_job(db, job_id=job_id)

    if not job:
        logger.warning(f"User {user_id} tried to access job {job_id} that does not exist.")
        raise HTTPException(status_code=404, detail="Job not found")

    if job.user_id != user_id and not is_admin(user_data):
        logger.warning(f"User {user_id} tried to access job {job_id} that is not theirs.")
        raise HTTPException(status_code=403, detail="This job is not yours!")
    return job    

### (TODO) : Send the request to manager service to cancel job if it is running
@app.delete("/jobs/{job_id}", tags=["Jobs"])
async def abort_job(
    job_id : uuid.UUID,
    user_data : dict = Depends(get_current_user_id),
    db: Session = Depends(get_db)
):
    '''
    abort a job execution
    '''
    current_user_id = user_data["user_id"]
    isAdmin = True if is_admin(user_data) else False

    job = crud.get_job(db, job_id=job_id)
    if not job:
        logger.info(f"User {current_user_id} tried to delete job {job_id} that does not exist.")
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in ["SUBMITTED", "RUNNING"]:
        # Send abort request to Manager — this kills K8s pods and marks tasks as FAILED
        try:
            async with httpx.AsyncClient() as client:
                abort_response = await client.delete(
                    f"http://manager-service:8000/manager/jobs/{job_id}",
                    timeout=10.0
                )
                abort_response.raise_for_status()
                logger.info(f"Manager successfully aborted job {job_id}: {abort_response.json()}")
        except Exception as mgr_err:
            logger.warning(f"Manager abort request failed for job {job_id}: {mgr_err}. Proceeding with local cleanup.")
    
    if job.user_id != current_user_id and not isAdmin:
        logger.warning(f"User {current_user_id} tried to delete job {job_id} that is not theirs.")
        raise HTTPException(status_code=403, detail="This job is not yours to delete!")
    
    # Clean up intermediate MinIO files (partitions, intermediate results)
    # NOTE: The user's original uploads (under user-{id}/) are NOT deleted
    storage.delete_job_files(str(job_id))
    
    deleted = crud.delete_job(db, job_id=job_id)
    if deleted:
        if isAdmin:
            logger.info(f"Admin user {current_user_id} requested to delete job {job_id}.")
        else:
            logger.info(f"User {current_user_id} requested to delete job {job_id}.")
    else:
        logger.info(f"User {current_user_id} tried to delete job {job_id} and encountered an error on the system's part.")
        raise HTTPException(status_code=404, detail="Could not delete job for some reason.")

"""====================================================== 
                        ADMIN ENDPOINTS
 ====================================================== """

## !!! Important sidenote here !!
# The system doesn't provide user creation/deletion for admins, since this is done immediately via Keycloak.
# The admin themselves should log in to keycloak with their administrative rights, and handle such issues there.

### DONE FOR NOW
@app.get("/admin/jobs", response_model=List[schemas.JobResponse], tags=["Admin"])
async def get_all_jobs_admin(
    admin_id : str = Depends(get_admin_user), #at this point, if not admin, it should fail
    db: Session = Depends(get_db)
):
    '''
    admin ONLY: list all jobs in the system
    '''
    # since verification is now done, give all the jobs to user
    logger.info(f"Admin user {admin_id} requested to see all jobs.")
    return crud.get_all_jobs(db)

### TO GO TO MANAGER, NOT HERE
@app.post("/admin/nodes", tags=["Admin"])
async def configure_nodes(config: dict):
    '''
    admin ONLY: Configure worker nodes
    '''
    return {"message": "Node configuration updated"}

### (TODO) : To be filled later on in the project
@app.delete("/admin/users/{user_id}", tags=["Admin"])
async def delete_user_data( 
    user_id: str,
    admin_id: str = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    logger.info(f"Admin user {admin_id} is purging data for user: {user_id}.")

    '''
    admin ONLY: Delete a user's data, after cancelling all their active jobs.
    This concerns the DB and MinIO -ONLY-
    The user's account in Keycloak remains untouched, and has to be purged manually by the admin.

    Therefore, if an admin uses this function without deleting the user's keycloak account, then the 
    user will be able to log in again, but will not have any data.
    On the other hand, if the admin only deletes the user's keycloak account, then their
    data will still remain here, but the user will not be able to access it without a Keycloak token for the
    specific account.

    THIS is what this function takes care of. 
    When it's called :
    1) All the jobs that belong to the user and are currently running or submitted, are first cancelled by sending 
    a request to the manager service.
    2) Once all active jobs are cancelled, the user's data is deleted from the database.
    3) Finally, the user's data is deleted from MinIO.
    4) Then, the admin can externally visit Keycloak on their own and handle the user deletion from there.
    '''
    try:
        # 2) MinIO : Delete all folders named "user-{user_id}"
        deleted_files_count = storage.delete_user_files(user_id)
                    
        # 3) remove all job and task rows that are related with the user_id in the database
        # This will also delete any job-specific files in MinIO (intermediates, inputs, outputs)
        deleted_jobs_count = crud.delete_user_jobs(db, user_id)
        
    except Exception as e:
        logger.error(f"Error purging data for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to purge user data: {str(e)}")

    if deleted_jobs_count == 0 and deleted_files_count == 0:
        logger.warning(f"Admin {admin_id} tried to delete data for user {user_id}, but no data was found.")
        raise HTTPException(status_code=404, detail=f"User {user_id} not found, or doesn't have any active jobs or files.")
        
    logger.info(f"Successfully purged data for user {user_id}. Deleted {deleted_jobs_count} jobs and {deleted_files_count} files.")
    return {"message": f"User {user_id} data deleted successfully. {deleted_jobs_count} jobs and {deleted_files_count} files removed."}