from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt
from sqlalchemy.orm import Session
from typing import List
import uuid

from database import schemas, crud, storage
from database.db import get_db

app = FastAPI()

# now this tells fastAPI where to search for a token
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="http://localhost:8080/realms/MapReduce-Realm/protocol/openid-connect/token"
)

#this is keycloak's public key, which will be used to verify the token
keycloak_public_key = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4oY/Hum1Q5g36Q793jEacycTUMEGM/ZlTOOi4L+d8KLAPbu7qIwjzTNLfvnT9ZE4tYNEbgYtMhKeyP/YO66qs/WlwfdzsmvwceLuGdByLmndkwsGC3SooXwWQIfEuYPG+naUqvbM/djf938h/6WkYGtOYK0k4PsjtjMzou0jow+yEFgP7PPWI8DJUbpdsYaGZgHljBV3HOdL6YqoeVqjosw8Iylf9F11kSAQ6GARiJn7xa0CAZQh3QkzK3gf0iIuahp9P5GJySbQ/RY02sS5iaPDxcpAvjLrm1A8d0AuH2CrGPwUASLK+HNFwI6jOyf+h0EVoDKvLjAc8oGGbFgNFwIDAQAB
-----END PUBLIC KEY-----"""

# --- Auth & RBAC ---

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
        # This catches invalid signatures or tampered tokens
        raise HTTPException(status_code=401, detail=f"Token validation failed: {str(e)}")

# this is to extract only the user id
async def get_verified_user_id(user_data: dict = Depends(get_current_user_id)) -> str:
    return user_data["user_id"]

# this is more specific, for when we want only admin users
async def get_admin_user(user_data: dict = Depends(get_current_user_id)) -> str:
    if "admin" not in user_data["roles"]:
        raise HTTPException(
            status_code=403,
            detail="Admin role required"
        )
    return user_data["user_id"]


# --- ENDPOINTS ---

### DONE FOR NOW
@app.post("/files/upload", response_model=schemas.FileUploadResponse)
async def upload_files(
    input_data: UploadFile = File(...), 
    mapper_code: UploadFile = File(...), 
    reducer_code: UploadFile = File(...)
):
    '''
    1) upload files to minio -> submit input files and return pointers
    '''
    # we have no job creation yet, so for now generate a unique prefix for this set of files
    # this will be the job_id in the future, instead of a random string
    unique_prefix = str(uuid.uuid4())

    try:
        #read input in bytes
        input_bytes = await input_data.read()
        mapper_bytes = await mapper_code.read()
        reducer_bytes = await reducer_code.read()

        #now, upload them using functions in /database/storage.py
        input_ref = storage.upload_data_bytes("mapreduce-inputs", f"job-{unique_prefix}/input_data", input_bytes)
        mapper_ref = storage.upload_data_bytes("mapreduce-code", f"job-{unique_prefix}/mapper.py", mapper_bytes)
        reducer_ref = storage.upload_data_bytes("mapreduce-code", f"job-{unique_prefix}/reducer.py", reducer_bytes)

        return {
            "input_ref": input_ref,
            "mapper_ref": mapper_ref,
            "reducer_ref": reducer_ref
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during file upload: {str(e)}")
    
    

### TO DO !!
@app.post("/jobs", response_model=schemas.JobResponse)
async def submit_job(
    job_in: schemas.JobCreate, 
    user_id: str = Depends(get_verified_user_id)
):
    '''
    2) submit MapReduce job -> forward request to manager service
    '''
    # dummy logic, later on this will call crud.create_job() and notify the Manager
    return {
        "job_id": uuid.uuid4(),
        "user_id": user_id,
        "status": "SUBMITTED",
        "input_code_ref": job_in.input_code_ref,
        "mapper_code_ref": job_in.mapper_code_ref,
        "reducer_code_ref": job_in.reducer_code_ref,
        "created_at": "2026-04-04T12:00:00",
        "updated_at": "2026-04-04T12:00:00"
    }

### DONE FOR NOW
@app.get("/jobs", response_model=List[schemas.JobResponse])
async def get_user_jobs(user_id: str = Depends(get_verified_user_id), db: Session = Depends(get_db)):
    '''
    list all jobs for the authenticated user
    '''
    # get_current_user_id() --> dictionary with username and role.
    # get_verified_user_id() --> keeps only the user id
    # so this function now calls get_verified_user_id() to get the user id

    print(f"Debugging -- Requested all jobs for userID: {user_id}")
    return crud.get_jobs_for_user(db, user_id)

### DONE FOR NOW
@app.get("/jobs/{job_id}", response_model=schemas.JobDetailResponse)
async def get_job(
    job_id : uuid.UUID,
    user_id : str = Depends(get_verified_user_id),
    db: Session = Depends(get_db)
):
    job = crud.get_job(db, job_id=job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.user_id != user_id:
        raise HTTPException(status_code=403, detail="This job is not yours!")
    return job    

### DONE FOR NOW
@app.delete("/jobs/{job_id}")
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
        raise HTTPException(status_code=404, detail="Job not found")
    if job.user_id != user_id:
        raise HTTPException(status_code=403, detail="This job is not yours to delete!")
        
    deleted = crud.delete_job(db, job_id=job_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Could not delete job for some reason.")
    return {"message": f"Job {job_id} deletion requested"}

# --- ADMIN ENDPOINTS ---

## !!! Important sidenote here !!
# The system doesn't provide user creation/deletion for admins, since this is done immediately via Keycloak.
# The admin themselves should log in to keycloak with their administrative rights, and handle such issues there.

### DONE FOR NOW
@app.get("/admin/jobs", response_model=List[schemas.JobResponse])
async def get_all_jobs_admin(
    admin_id : str = Depends(get_admin_user), #at this point, if not admin, it should fail
    db: Session = Depends(get_db)
):
    '''
    admin ONLY: list all jobs in the system
    '''
    # since verification is now done, give all the jobs to user
    print(f"Debugging --- Admin user {admin_id} requested to see all jobs")
    return crud.get_all_jobs(db)

### TO GO TO MANAGER, NOT HERE
@app.post("/admin/nodes")
async def configure_nodes(config: dict):
    '''
    admin ONLY: Configure worker nodes
    '''
    return {"message": "Node configuration updated"}

### TO DO !!!
@app.delete("/admin/users/{user_id}")
async def delete_user_data(user_id: str):
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
    return {"message": f"User {user_id} deletion requested"}