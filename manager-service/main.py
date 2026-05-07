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
    
    # Environment variables are the ONLY way the worker knows its   job
    env_vars = [
        client.V1EnvVar(name="TASK_ID", value=str(task.task_id)),
        client.V1EnvVar(name="JOB_ID", value=str(job.job_id)),
        # FIX: Add .value to ensure we send "MAP" instead of TaskType.MAP
        client.V1EnvVar(name="TASK_TYPE", value=str(task.task_type.value)), 
        client.V1EnvVar(name="INPUT_REF", value=task.input_partition_ref),
        client.V1EnvVar(
            name="CODE_REF", 
            # FIX: Also ensure these refs are strings if they come from an Enum
            value=str(job.mapper_code_ref) if task.task_type.value == "MAP" else str(job.reducer_code_ref)
        ),
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

        # Data Partitioning — num_mappers comes from the Job record (user-configurable)
        num_mappers = job.num_mappers or 3
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

@app.post("/manager/jobs")
def trigger_job(
    payload: dict, # Expecting {"job_id": "uuid-string"}
    background_tasks: BackgroundTasks,
    db_session: Session = Depends(db.get_db)
):
    """
    Step 1 & 2 Trigger:
    The UI creates the job entry first, then pings this endpoint to start
    partitioning and pod spawning.
    """
    job_id = payload.get("job_id")
    if not job_id:
        raise HTTPException(status_code=400, detail="Missing job_id in payload")

    logger.info(f"[*] Manager received trigger for Job ID: {job_id}")

    # 1. Verify the job exists in the shared PostgreSQL DDS
    job = crud.get_job(db_session, job_id)
    if not job:
        logger.error(f"[!] Job {job_id} not found in database.")
        raise HTTPException(status_code=404, detail="Job not found in DB")
    
    # 2. Hand off to the Orchestration Brain (Partitioning -> Pod Spawning)
    # This runs in the background so the UI doesn't time out
    background_tasks.add_task(start_job_orchestration, job.job_id, db_session)
    
    logger.info(f"[✓] Orchestration background task scheduled for Job: {job_id}")
    return {"message": "Orchestration started", "job_id": job_id}

@app.post("/manager/tasks/status", response_model=schemas.TaskResponse)
def update_task_status(update: schemas.TaskStatusUpdate, db_session: Session = Depends(db.get_db)):
    """
    Receives status updates from Workers and orchestrates phase transitions.
    """
    logger.info(f"Updating task {update.task_id} to status {update.status}")
    
    # 1. Update the individual task in the DB
    updated_task = crud.update_task_status(
        db=db_session,
        task_id=str(update.task_id),
        new_status=update.status,
        worker_pod_id=update.worker_pod_id,
        output_partition_ref=update.output_partition_ref
    )
    
    if not updated_task:
        raise HTTPException(status_code=404, detail="Task not found")

    # 2.Check if we should move from MAP to REDUCE
    if updated_task.status == db.TaskStatus.COMPLETED and updated_task.task_type == db.TaskType.MAP:
        job_id = str(updated_task.job_id)
        
        # Check if all other MAP tasks for this job are finished
        all_tasks = crud.get_tasks_for_job(db_session, job_id, task_type=db.TaskType.MAP)
        if all(t.status == db.TaskStatus.COMPLETED for t in all_tasks):
            logger.info(f"[✓] All Mappers for job {job_id} completed. Starting Shuffle/Reduce...")

            try:
                # 3. SHUFFLE PHASE: Partition intermediate outputs
                job = crud.get_job(db_session, job_id)
                num_reducers = job.num_reducers or 1
                intermediate_refs = [t.output_partition_ref for t in all_tasks]
                shuffled_refs = storage.shuffle_intermediate_results(job_id, intermediate_refs, num_reducers)
                logger.info(f"[✓] Shuffle complete. {len(shuffled_refs)} partitions ready.")

                # 4. REDUCE PHASE: Create tasks and spawn pods
                for ref in shuffled_refs:
                    reducer_task = crud.create_task(
                        db=db_session,
                        job_id=job_id,
                        task_type=db.TaskType.REDUCE,
                        input_partition_ref=ref
                    )
                    
                    create_worker_pod(reducer_task, job)
                    logger.info(f"[+] Reducer pod spawned for task {reducer_task.task_id}")

            except Exception as e:
                logger.error(f"[X] Failed to transition to Reduce phase for {job_id}: {e}")
                crud.update_job_status(db_session, job_id, db.JobStatus.FAILED)

    # 5. FINAL COMPLETION: If ALL REDUCE tasks finish, the whole job is done
    elif updated_task.status == db.TaskStatus.COMPLETED and updated_task.task_type == db.TaskType.REDUCE:
        job_id = str(updated_task.job_id)
        
        all_reduce_tasks = crud.get_tasks_for_job(db_session, job_id, task_type=db.TaskType.REDUCE)
        if all(t.status == db.TaskStatus.COMPLETED for t in all_reduce_tasks):
            crud.update_job_status(db_session, job_id, db.JobStatus.COMPLETED)
            crud.update_job_output_ref(db_session, job_id, f"mapreduce-intermediates/job-{job_id}/")
            logger.info(f"--- [!!!] JOB {job_id} FULLY COMPLETED [!!!] ---")
    
    return updated_task

@app.delete("/manager/jobs/{job_id}")
def abort_job(job_id: str, db_session: Session = Depends(db.get_db)):
    """
    Abort a running job:
    1. Kill all active Kubernetes worker pods for this job
    2. Mark all tasks as FAILED
    3. Mark the job as FAILED
    
    Called by the UI service when a user requests job cancellation.
    """
    logger.info(f"[*] Abort requested for Job: {job_id}")

    job = crud.get_job(db_session, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # 1. Find all tasks for this job and kill their K8s Jobs
    tasks = crud.get_tasks_for_job(db_session, job_id)
    killed_count = 0

    for task in tasks:
        pod_name = f"worker-{task.task_id}"
        
        # Try to delete the K8s Job (if it exists and is still running)
        if k8s_batch_v1:
            try:
                k8s_batch_v1.delete_namespaced_job(
                    name=pod_name,
                    namespace="default",
                    body=client.V1DeleteOptions(propagation_policy="Background")
                )
                killed_count += 1
                logger.info(f"[✓] Killed K8s Job: {pod_name}")
            except Exception as e:
                # Pod might already be done or not exist — that's fine
                logger.warning(f"[!] Could not kill K8s Job {pod_name}: {e}")

        # 2. Mark each task as FAILED
        if task.status not in [db.TaskStatus.COMPLETED, db.TaskStatus.FAILED]:
            crud.update_task_status(db_session, str(task.task_id), db.TaskStatus.FAILED)

    # 3. Mark the job itself as FAILED
    crud.update_job_status(db_session, job_id, db.JobStatus.FAILED)
    
    logger.info(f"[✓] Job {job_id} aborted. Killed {killed_count} K8s pods, marked {len(tasks)} tasks as FAILED.")
    return {
        "message": f"Job {job_id} aborted successfully",
        "killed_pods": killed_count,
        "failed_tasks": len(tasks)
    }

@app.get("/healthz")
def health_check():
    """Liveness probe for Kubernetes orchestration """
    return {"status": "ok"}