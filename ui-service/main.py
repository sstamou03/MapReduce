from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt
from sqlalchemy.orm import Session
from typing import List
import uuid

from database import schemas, crud, storage
from database.db import get_db

# we configure logging to see it in terminal
import logging
import colorlog

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

app = FastAPI()

# now this tells fastAPI where to search for a token
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="http://localhost:8080/realms/MapReduce-Realm/protocol/openid-connect/token"
)

#this is keycloak's public key, which will be used to verify the token
keycloak_public_key = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4oY/Hum1Q5g36Q793jEacycTUMEGM/ZlTOOi4L+d8KLAPbu7qIwjzTNLfvnT9ZE4tYNEbgYtMhKeyP/YO66qs/WlwfdzsmvwceLuGdByLmndkwsGC3SooXwWQIfEuYPG+naUqvbM/djf938h/6WkYGtOYK0k4PsjtjMzou0jow+yEFgP7PPWI8DJUbpdsYaGZgHljBV3HOdL6YqoeVqjosw8Iylf9F11kSAQ6GARiJn7xa0CAZQh3QkzK3gf0iIuahp9P5GJySbQ/RY02sS5iaPDxcpAvjLrm1A8d0AuH2CrGPwUASLK+HNFwI6jOyf+h0EVoDKvLjAc8oGGbFgNFwIDAQAB
-----END PUBLIC KEY-----"""


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
            keycloak_public_key, #used to verify token that was signed with Keycloak's private key
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

async def is_admin(user_id: str) -> bool:
    """
    Check if the user is an admin.
    """
    user_data = get_current_user_id(user_id)
    return "admin" in user_data["roles"]


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

    try:
        new_job = crud.create_job(
            db=db,
            user_id=user_id,
            input_code_ref=job_in.input_code_ref,
            mapper_code_ref=job_in.mapper_code_ref,
            reducer_code_ref=job_in.reducer_code_ref
        )
        
        logger.info(f"User {user_id} submitted a new job {new_job.job_id}.")

        # next step here is to make the ui send a request to the manager service to start the job
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
    user_id : str = Depends(get_verified_user_id),
    db: Session = Depends(get_db)
):
    logger.info(f"User {user_id} requested details for job {job_id}.")
    job = crud.get_job(db, job_id=job_id)
    if not job:
        logger.warning(f"User {user_id} tried to access job {job_id} that does not exist.")
        raise HTTPException(status_code=404, detail="Job not found")
    if job.user_id != user_id:
        logger.warning(f"User {user_id} tried to access job {job_id} that is not theirs.")
        raise HTTPException(status_code=403, detail="This job is not yours!")
    return job    

### (TODO) : Send the request to manager service to cancel job if it is running
@app.delete("/jobs/{job_id}", tags=["Jobs"])
async def abort_job(
    job_id : uuid.UUID,
    user_id : str = Depends(get_verified_user_id),
    db: Session = Depends(get_db)
):
    '''
    abort a job execution
    '''
    job = crud.get_job(db, job_id=job_id)
    if not job:
        logger.warning(f"User {user_id} tried to delete job {job_id} that does not exist.")
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in ["SUBMITTED", "RUNNING"]:
        ## fill this with the request to manager service to nuke the job (pun intended)
        pass
    
    is_admin = True if is_admin(user_id) else False

    if job.user_id != user_id and not is_admin:
        logger.warning(f"User {user_id} tried to delete job {job_id} that is not theirs.")
        raise HTTPException(status_code=403, detail="This job is not yours to delete!")
    
    # --- REMEMBER TO FILL THIS PAAAAAAAART ------
    # here we should send a request to the manager service to delete the job
    # if the job is completed/failed, we can delete it from the database
    # if the job is running/submitted/retrying/pending, we should send a request to the manager service to abort the job first
    # then we can delete it from the database
    
    deleted = crud.delete_job(db, job_id=job_id)

    if is_admin(user_id):
        logger.info(f"Admin user {user_id} requested to delete job {job_id}.")
    else:
        logger.info(f"User {user_id} requested to delete job {job_id}.")
    
    if not deleted:
        raise HTTPException(status_code=404, detail="Could not delete job for some reason.")
    return {"message": f"Job {job_id} deletion requested"}

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

### (TODO) : Write the rest at some point huh
@app.delete("/admin/users/{user_id}", tags=["Admin"])
async def delete_user_data( 
    admin_id: str = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    '''
    admin ONLY: Delete a user's data.
    This concerns the DB and MinIO -ONLY-
    The user's account in Keycloak remains untouched.
    Therefore, if an admin uses this function without deleting the user's keycloak account, then the 
    user will be able to log in again, but will not have any data.
    On the other hand, if the admin only deletes the user's keycloak account, then their
    data will still remain here, but the user will not be able to access it without a Keycloak token for the
    specific account.

    THIS is what this function takes care of. 
    When the admin calls it, it purges the system (postgresql & minio) from the specific user, so that then they
    can be deleted from keycloak as well.
    '''
    if admin_id == user_id:
        raise HTTPException(status_code=403, detail="You cannot delete your own account!")

    if not is_admin(user_id):
        raise HTTPException(status_code=403, detail="You are not an admin!")

    

    return {"message": f"User {user_id} deletion requested"}