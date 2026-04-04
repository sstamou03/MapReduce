from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from typing import List
import uuid

from database import schemas

app = FastAPI()

# --- AUTHENTICATION PLACEHOLDER ---

''' in the following weeks, we'll replace this with a functionality that validates
the keycloak JWT and extracts the user_id '''

async def get_current_user_id():
    # this will later use jose to validate jwt and extract user id
    return "user-12345-demo"

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
    user_id: str = Depends(get_current_user_id)
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
async def get_jobs(user_id: str = Depends(get_current_user_id)):
    '''
    list all jobs for the authenticated user
    '''
    return [] # dummy empty list for now

@app.get("/jobs/{job_id}", response_model=schemas.JobDetailResponse)
async def get_job_status(job_id: uuid.UUID):
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