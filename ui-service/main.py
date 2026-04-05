from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt
from sqlalchemy.orm import Session
from typing import List
import uuid

from database import schemas, crud
from database.db import get_db

app = FastAPI()

# now this tells fastAPI where to search for a token
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="http://localhost:8080/realms/MapReduce-Realm/protocol/openid-connect/token"
)

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
            keycloak_public_key, 
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

# --- ENDPOINTS ---

@app.post("/files/upload", response_model=schemas.FileUploadResponse)
async def upload_files(
    input_data: UploadFile = File(...), 
    mapper_code: UploadFile = File(...), 
    reducer_code: UploadFile = File(...)
):
    '''
    1) upload files to minio -> submit input files and return pointers
    '''
    # dummy logic, later on this will use storage.upload_input_data()
    return {
        "input_ref": f"mapreduce-inputs/{input_data.filename}",
        "mapper_ref": f"mapreduce-code/{mapper_code.filename}",
        "reducer_ref": f"mapreduce-code/{reducer_code.filename}"
    }

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

@app.get("/jobs/{job_id}", response_model=schemas.JobDetailResponse)
async def get_job(job_id: uuid.UUID):
    '''
    check the status of a specific job
    '''

    # dummy response matching the design's JobDetailResponse
    return {
        "job_id": job_id,
        "user_id": "user-12345-demo",
        "status": "RUNNING",
        "input_code_ref": "ref/data",
        "mapper_code_ref": "ref/mapper",
        "reducer_code_ref": "ref/reducer",
        "created_at": "2026-04-04T12:00:00",
        "updated_at": "2026-04-04T12:05:00",
        "tasks": []
    }

@app.delete("/jobs/{job_id}")
async def abort_job(job_id: uuid.UUID):
    '''
    abort a job execution
    '''
    return {"message": f"Job {job_id} deletion requested"}

# --- ADMIN ENDPOINTS ---

@app.get("/admin/jobs", response_model=List[schemas.JobResponse])
async def get_all_jobs_admin(db: Session = Depends(get_db)):
    '''
    admin ONLY: list all jobs in the system
    '''
    # add logic here that we verify the user is admin via jwt
    return crud.get_all_jobs(db)

@app.post("/admin/nodes")
async def configure_nodes(config: dict):
    '''
    admin ONLY: Configure worker nodes
    '''
    return {"message": "Node configuration updated"}

@app.delete("/admin/users/{user_id}")
async def delete_user(user_id: str):
    '''
    admin ONLY: Delete a user --> this will be passed through keycloak
    '''
    return {"message": f"User {user_id} deletion requested"}

@app.post("/admin/users")
async def create_user(user: dict):
    '''
    admin ONLY: Create a user --> this will be passed through keycloak
    '''
    return {"message": "User created"}