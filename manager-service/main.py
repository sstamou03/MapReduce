import logging
import uuid
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Header
from sqlalchemy.orm import Session
import os
from kubernetes import client, config

from database import crud, schemas, db, storage

# Setup logging for monitoring the partitioning process
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger("manager-service")

app = FastAPI(title="Manager Service")

# 2. Initialize Kubernetes Client
try:
    # Detect if we are in Minikube or running locally
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        config.load_incluster_config()
        logger.info("[✓] Connected to Kubernetes Internal API.") 
    else:
        config.load_kube_config()
        logger.info("[✓] Connected via local Kubeconfig (Minikube).")
    
    # We use BatchV1Api because Workers are implemented as K8s 'Jobs'
    k8s_batch_v1 = client.BatchV1Api()
    logger.info("[✓] Kubernetes client initialized.")
except Exception as e:
    logger.warning(f"[!] K8s client not ready (ignore if Minikube isn't running yet): {e}")
    k8s_batch_v1 = None

def create_worker_pod(task: schemas.TaskResponse, job: schemas.JobResponse):
    """
    Translates a DB Task into a Kubernetes Job.
    Ensures environment variables match the Worker's requirements.
    """
    # Unique name for the K8s Job resource
    pod_name = f"worker-{task.task_id}"
    
    # Environment variables are the ONLY way the worker knows its job
    env_vars = [
        client.V1EnvVar(name="TASK_ID", value=str(task.task_id)),
        client.V1EnvVar(name="JOB_ID", value=str(job.job_id)),
        client.V1EnvVar(name="TASK_TYPE", value=task.task_type),
        client.V1EnvVar(name="INPUT_REF", value=task.input_partition_ref),
        # Map tasks use mapper_code_ref, Reduce tasks use reducer_code_ref 
        client.V1EnvVar(
            name="CODE_REF", 
            value=job.mapper_code_ref if task.task_type == "MAP" else job.reducer_code_ref
        ),
        # Internal K8s service name for the manager
        client.V1EnvVar(name="MANAGER_URL", value="http://manager-service:8000"),
    ]

    # Define the container using our worker image 
    container = client.V1Container(
        name="worker",
        image="worker-service:latest", 
        env=env_vars
    )

    # Define the Pod template 
    template = client.V1PodTemplateSpec(
        spec=client.V1PodSpec(containers=[container], restart_policy="Never")
    )

    # Define the Job specification 
    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=3  # Fault tolerance: retry 3 times if it crashes 
    )

    # Create the final Job object
    k8s_job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=pod_name),
        spec=job_spec
    )

    try:
        if k8s_batch_v1:
            k8s_batch_v1.create_namespaced_job(namespace="default", body=k8s_job)
            logger.info(f"[✓] Dynamically spawned K8s Job: {pod_name}")
    except Exception as e:
        logger.error(f"[X] Failed to spawn K8s Job for task {task.task_id}: {e}")

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

# 3. Create Tasks AND Launch Pods in ONE loop 
        for i, ref in enumerate(partition_refs):
            new_task = crud.create_task(
                db=db_session,
                job_id=job_str,
                task_type=db.TaskType.MAP,
                input_partition_ref=ref
            )
            logger.info(f"[+] Task {i} Created: {new_task.task_id}")
            
            # Immediately tell K8s to run a worker for this specific task
            create_worker_pod(new_task, job)
        
        logger.info(f"[*] All {len(partition_refs)} Mapper pods successfully requested from K8s API.")

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