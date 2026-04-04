import logging
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from database import crud, schemas, db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Manager Service")

@app.post("/manager/jobs", response_model=schemas.JobResponse)
def schedule_job(job_data: schemas.JobCreate, db_session: Session = Depends(db.get_db)):
    """
    Receives job information from the UI service.
    Stores metadata in PostgreSQL with status SUBMITTED.
    """
    pass

@app.post("/manager/tasks/status", response_model=schemas.TaskResponse)
def update_task_status(update: schemas.TaskStatusUpdate, db_session: Session = Depends(db.get_db)):
    """
    Receives status updates from Workers (e.g., task completed/failed).
    Updates the task state in PostgreSQL.
    """
    pass

@app.get("/healthz")
def health_check():
    return {"status": "ok"}