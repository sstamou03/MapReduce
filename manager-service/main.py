import logging
import uuid
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Header
from sqlalchemy.orm import Session

from database import crud, schemas, db, storage

# Setup logging for monitoring the partitioning process
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger("manager-service")

app = FastAPI(title="Manager Service")

def start_job_orchestration(job_id: uuid.UUID, db_session: Session):
    """
    Background logic for partitioning and task creation.
    """
    job_str = str(job_id)
    logger.info(f"[*] Starting background orchestration for Job: {job_str}")

    try:
        job = crud.get_job(db_session, job_str)
        if not job:
            logger.error(f"[!] Job {job_str} not found.")
            return

        # Update status to RUNNING
        crud.update_job_status(db_session, job_str, db.JobStatus.RUNNING)
        logger.info(f"[✓] Job {job_str} (User: {job.user_id}) is now RUNNING.")

        # Data Partitioning
        num_mappers = 3 
        partition_refs = storage.split_and_upload_input(
            job_id=job_str,
            input_ref=job.input_code_ref,
            num_mappers=num_mappers
        )
        logger.info(f"[✓] Partitioned into {len(partition_refs)} chunks.")

        # Create MAP tasks
        for ref in partition_refs:
            crud.create_task(
                db=db_session,
                job_id=job_str,
                task_type=db.TaskType.MAP,
                input_partition_ref=ref
            )
        
        logger.info(f"[*] Tasks for {job_str} are ready in DDS. Proceeding to K8s logic.")

    except Exception as e:
        logger.error(f"[X] Orchestration error for {job_str}: {str(e)}")
        crud.update_job_status(db_session, job_str, db.JobStatus.FAILED)


@app.post("/manager/jobs", response_model=schemas.JobResponse)
def schedule_job(
    job_data: schemas.JobCreate, 
    background_tasks: BackgroundTasks,
    db_session: Session = Depends(db.get_db),
    x_user_id: str = Header(...) # Identifies the user from Keycloak 
):
    """
    Receives job information from the UI service.
    Saves metadata and triggers background partitioning.
    """
    logger.info(f"[*] UI triggered a new job for User: {x_user_id}")

    # Use the real user_id from the header 
    new_job = crud.create_job(
        db=db_session,
        user_id=x_user_id,
        input_code_ref=job_data.input_code_ref,
        mapper_code_ref=job_data.mapper_code_ref,
        reducer_code_ref=job_data.reducer_code_ref
    )
    
    # Trigger partitioning logic in background
    background_tasks.add_task(start_job_orchestration, new_job.job_id, db_session)
    
    return new_job

@app.post("/manager/tasks/status", response_model=schemas.TaskResponse)
def update_task_status(update: schemas.TaskStatusUpdate, db_session: Session = Depends(db.get_db)):
    """
    Receives status updates from Workers (e.g., task completed/failed).
    Updates the task state in PostgreSQL.
    """
    logger.info(f"Updating task {update.task_id} to status {update.status}")
    updated_task = crud.update_task_status(
        db=db_session,
        task_id=str(update.task_id),
        new_status=update.status,
        worker_pod_id=update.worker_pod_id,
        output_partition_ref=update.output_partition_ref
    )
    
    if not updated_task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return updated_task

@app.get("/healthz")
def health_check():
    """Liveness probe for Kubernetes orchestration """
    return {"status": "ok"}