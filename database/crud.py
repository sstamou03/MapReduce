from sqlalchemy.orm import Session
from .db import Job, Task, JobStatus, TaskStatus, TaskType

"""
Data Access Layer (CRUD) for the MapReduce platform.
This module hides raw SQLAlchemy queries from the rest of the application.
"""

#jobs

def create_job(db: Session, user_id: str, input_code_ref: str, mapper_code_ref: str, reducer_code_ref: str) -> Job:
    """
    Called by the UI service (POST /jobs) or Manager service when a new job is submitted.
    Creates a new Job record in the database with status SUBMITTED.
    """

    new_job = Job(
        user_id=user_id,
        status=JobStatus.SUBMITTED,
        input_code_ref=input_code_ref,
        mapper_code_ref=mapper_code_ref,
        reducer_code_ref=reducer_code_ref
    )
    db.add(new_job)
    db.commit()
    db.refresh(new_job)
    return new_job

def get_job(db: Session, job_id: str) -> Job:
    """
    Called by the UI service (GET /jobs/{job_id}) or Manager to retrieve a specific job.
    """
    return db.query(Job).filter(Job.job_id == job_id).first()

def get_all_jobs(db: Session):
    """
    Called by the UI service (for admins) to list all jobs in the system.
    """
    return db.query(Job).all()

def get_jobs_for_user(db: Session, user_id: str):
    """
    Called by the UI service (GET /jobs) to list all jobs belonging to the authenticated user.
    """
    return db.query(Job).filter(Job.user_id == user_id).all()

def update_job_status(db: Session, job_id: str, new_status: JobStatus) -> Job:
    """
    Called by the Manager service when a job transitions between states 
    (e.g., from SUBMITTED to RUNNING, or RUNNING to COMPLETED).
    """

    job = get_job(db, job_id)
    if job:
        job.status = new_status
        db.commit()
        db.refresh(job)
    return job

def delete_job(db: Session, job_id: str) -> bool:
    """
    Called by the UI service (DELETE /jobs/{job_id}) to abort a job.
    Deletes all tasks linked to the job first, then deletes the job itself.
    Returns True if the job was found and deleted, False otherwise.
    """

    job = get_job(db, job_id)
    if not job:
        return False
    # Delete all tasks that belong to this job first
    db.query(Task).filter(Task.job_id == job_id).delete()
    db.delete(job)
    db.commit()
    return True

def update_job_output_ref(db: Session, job_id: str, output_code_ref: str) -> Job:
    """
    Called by the Manager service when all reducers finish successfully.
    Saves the final output MinIO reference so the user can retrieve the result.
    """

    job = get_job(db, job_id)
    if job:
        job.output_code_ref = output_code_ref
        db.commit()
        db.refresh(job)
    return job

# NOTE: 
# The POST /admin/nodes endpoint is NOT handled here.
# Node configuration should be managed by the Manager service (e.g., in-memory or via Kubernetes ConfigMaps),
# not through using the db.We dont have any table for this.

#tasks

def create_task(db: Session, job_id: str, task_type: TaskType, input_partition_ref: str) -> Task:
    """
    Called by the Manager service during Job Execution to create Mapper or Reducer tasks.
    """

    new_task = Task(
        job_id=job_id,
        task_type=task_type,
        status=TaskStatus.PENDING,
        input_partition_ref=input_partition_ref
    )
    db.add(new_task)
    db.commit()
    db.refresh(new_task)
    return new_task

def get_tasks_for_job(db: Session, job_id: str, task_type: TaskType = None):
    """
    Called by the Manager service to monitor the progress of a job.
    Optionally filter by task_type (e.g., all MAP tasks).
    """

    query = db.query(Task).filter(Task.job_id == job_id)
    if task_type:
        query = query.filter(Task.task_type == task_type)
    return query.all()

def update_task_status(db: Session, task_id: str, new_status: TaskStatus, worker_pod_id: str = None, output_partition_ref: str = None) -> Task:
    """
    Called by the Manager service when it receives status updates from Workers 
    (POST /manager/tasks/status) or when a pod fails (to mark it as FAILED/RETRYING).
    Updates status, and optionally saves the worker ID (if RUNNING) or the output reference (if COMPLETED).
    """

    task = db.query(Task).filter(Task.task_id == task_id).first()
    if task:
        task.status = new_status
        if worker_pod_id is not None:
            task.worker_pod_id = worker_pod_id
        if output_partition_ref is not None:
            task.output_partition_ref = output_partition_ref
            
        db.commit()
        db.refresh(task)
    return task
