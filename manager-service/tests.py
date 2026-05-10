import unittest
import uuid
import sys
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

# 1. Mock psycopg2 to prevent connection errors during unit testing
sys.modules['psycopg2'] = MagicMock()

# Import your app and DB enums
from main import app, start_job_orchestration
from database import db

client = TestClient(app)

class TestManagerOrchestration(unittest.TestCase):

    def setUp(self):
        """Prepare common test data for each test run."""
        self.job_id = uuid.uuid4()
        self.db_session = MagicMock()

    # --- TEST: ENDPOINT TRIGGER ---
    @patch("database.crud.update_job_status") # Add this
    @patch("database.crud.get_job")
    def test_trigger_job_endpoint(self, mock_get_job, mock_update_status): # Add arg
        """
        Verifies that the POST /manager/jobs endpoint correctly 
        receives a job_id and triggers background logic.
        """
        mock_job = MagicMock()
        mock_job.job_id = self.job_id
        mock_get_job.return_value = mock_job

        response = client.post(
            "/manager/jobs", 
            json={"job_id": str(self.job_id)}
        )

        self.assertEqual(response.status_code, 200)

    # --- TEST: STEP 1 (PARTITIONING) ---
    @patch("database.crud.get_job")
    @patch("database.crud.update_job_status")
    @patch("database.storage.split_and_upload_input")
    @patch("database.crud.create_task")
    @patch("main.create_worker_pod") 
    def test_step1_partitioning_and_ledger(self, mock_pod, mock_create_task, mock_split, mock_update_status, mock_get_job):
        """
        Verifies partitioning logic and that we create 
        exactly one DB task per partition (no double loops!).
        """
        mock_job = MagicMock()
        mock_job.job_id = self.job_id
        mock_get_job.return_value = mock_job
        mock_split.return_value = ["ref0", "ref1", "ref2"]

        # Run the internal orchestration logic
        start_job_orchestration(self.job_id, self.db_session)

        # Confirm job status was set to RUNNING
        mock_update_status.assert_called_with(self.db_session, str(self.job_id), db.JobStatus.RUNNING)
        
        # Confirm we created EXACTLY 3 tasks in the DB
        self.assertEqual(mock_create_task.call_count, 3)
        print("[OK] Step 1 Test: Partitioning and Single-Loop Task Ledger verified.")

    # --- TEST: STEP 2 (K8s SPAWNING) ---
    @patch("main.k8s_batch_v1")
    @patch("database.crud.get_job")
    @patch("database.crud.update_job_status")
    @patch("database.storage.split_and_upload_input")
    @patch("database.crud.create_task")
    def test_step2_k8s_spawning(self, mock_create_task, mock_split, mock_update_status, mock_get_job, mock_k8s):
        """
        Verifies that for every partition, the Manager 
        dynamically requests a Pod from the Kubernetes API.
        """
        mock_job = MagicMock()
        mock_job.job_id = self.job_id
        mock_job.mapper_code_ref = "minio/mapper.py"
        mock_get_job.return_value = mock_job
        mock_split.return_value = ["ref0", "ref1", "ref2"]
        
       
# Mock task creation to return valid objects for pod naming
        def side_effect_create_task(*args, **kwargs):
            task = MagicMock()
            task.task_id = uuid.uuid4()
            # FIX: Use the Enum instead of a raw string "MAP"
            task.task_type = db.TaskType.MAP 
            task.input_partition_ref = kwargs.get('input_partition_ref', "ref")
            return task
        mock_create_task.side_effect = side_effect_create_task

        start_job_orchestration(self.job_id, self.db_session)

        # Verify K8s API was called 3 times
        self.assertEqual(mock_k8s.create_namespaced_job.call_count, 3)
        
        # Verify environment variables include the TASK_TYPE
        job_body = mock_k8s.create_namespaced_job.call_args_list[0][1]['body']
        env_vars = job_body.spec.template.spec.containers[0].env
        task_type_env = next(e for e in env_vars if e.name == "TASK_TYPE")
        self.assertEqual(task_type_env.value, "MAP")
        
        print("[OK] Step 2 Test: Dynamic Pod Spawning verified.")

    def test_healthz(self):
        """Check the Kubernetes Liveness probe."""
        response = client.get("/healthz")
        self.assertEqual(response.status_code, 200)

    # --- TEST: ABORT JOB ---
    @patch("main.k8s_batch_v1")
    @patch("database.crud.update_job_status")
    @patch("database.crud.update_task_status")
    @patch("database.crud.get_tasks_for_job")
    @patch("database.crud.get_job")
    def test_abort_job(self, mock_get_job, mock_get_tasks, mock_update_task, mock_update_job, mock_k8s):
        """
        Verifies that DELETE /manager/jobs/{job_id}:
        1. Finds all tasks for the job
        2. Attempts to kill K8s Jobs for each task
        3. Marks running tasks as FAILED
        4. Marks the job as FAILED
        """
        mock_job = MagicMock()
        mock_job.job_id = self.job_id
        mock_job.status = db.JobStatus.RUNNING
        mock_get_job.return_value = mock_job

        # Create 3 mock tasks (2 running, 1 completed)
        task1 = MagicMock()
        task1.task_id = uuid.uuid4()
        task1.status = db.TaskStatus.RUNNING

        task2 = MagicMock()
        task2.task_id = uuid.uuid4()
        task2.status = db.TaskStatus.RUNNING

        task3 = MagicMock()
        task3.task_id = uuid.uuid4()
        task3.status = db.TaskStatus.COMPLETED  # Already done, should NOT be updated

        mock_get_tasks.return_value = [task1, task2, task3]

        response = client.delete(f"/manager/jobs/{self.job_id}")

        # Endpoint should return 200
        self.assertEqual(response.status_code, 200)

        # K8s should be called 3 times (tries to delete all pods)
        self.assertEqual(mock_k8s.delete_namespaced_job.call_count, 3)

        # Only 2 tasks should be marked as FAILED (the COMPLETED one is skipped)
        self.assertEqual(mock_update_task.call_count, 2)

        # Job should be marked as FAILED
        mock_update_job.assert_called_with(
            unittest.mock.ANY, str(self.job_id), db.JobStatus.FAILED
        )

        data = response.json()
        self.assertEqual(data["killed_pods"], 3)
        self.assertEqual(data["failed_tasks"], 3)

        print("[OK] Abort Test: Pod killing and status updates verified.")

    # --- TEST: CUSTOM NUM_MAPPERS ---
    @patch("database.crud.get_job")
    @patch("database.crud.update_job_status")
    @patch("database.storage.split_and_upload_input")
    @patch("database.crud.create_task")
    @patch("main.create_worker_pod")
    def test_custom_num_mappers(self, mock_pod, mock_create_task, mock_split, mock_update_status, mock_get_job):
        """
        Verifies that num_mappers from the Job record is used
        instead of the old hardcoded value of 3.
        """
        mock_job = MagicMock()
        mock_job.job_id = self.job_id
        mock_job.num_mappers = 5  # User requested 5 mappers
        mock_get_job.return_value = mock_job
        mock_split.return_value = ["ref0", "ref1", "ref2", "ref3", "ref4"]

        start_job_orchestration(self.job_id, self.db_session)

        # split_and_upload_input should be called with num_mappers=5
        mock_split.assert_called_once()
        call_kwargs = mock_split.call_args
        self.assertEqual(call_kwargs[1]["num_mappers"], 5)

        # Should create exactly 5 tasks, not 3
        self.assertEqual(mock_create_task.call_count, 5)
        self.assertEqual(mock_pod.call_count, 5)

        print("[OK] Custom num_mappers test: 5 mappers created instead of default 3.")

        # --- TEST: PHASE PROGRESSION (MAP -> REDUCE) ---
    @patch("database.storage.shuffle_intermediate_results")
    @patch("database.crud.get_tasks_for_job")
    @patch("database.crud.get_job")
    @patch("main.create_worker_pod")
    @patch("database.crud.create_task")
    def test_transition_to_reduce_phase(self, mock_create_task, mock_pod, mock_get_job, mock_get_tasks, mock_shuffle):
        """
        Verifies that when the last Mapper completes, the Manager 
        automatically triggers Shuffle and spawns Reducers [cite: 130-132].
        """
        from main import handle_phase_progression
        
        # Setup: All tasks are COMPLETED Mappers
        task_mock = MagicMock(status=db.TaskStatus.COMPLETED, task_type=db.TaskType.MAP, output_partition_ref="ref")
        mock_get_tasks.return_value = [task_mock]
        mock_get_job.return_value = MagicMock(num_reducers=1)
        mock_shuffle.return_value = ["shuffled_ref_1"]
        mock_create_task.return_value = MagicMock(task_id=uuid.uuid4(), task_type=db.TaskType.REDUCE)

        # Run logic
        import asyncio
        asyncio.run(handle_phase_progression(self.db_session, str(self.job_id)))

        # Assertions
        mock_shuffle.assert_called_once() # Ensure Shuffle was triggered
        mock_create_task.assert_called()   # Ensure Reducer task created in DB
        self.assertEqual(mock_pod.call_count, 1) # Ensure Reducer pod spawned in K8s

    # --- TEST: RECOVERY LOGIC (SELF-HEALING) ---
    @patch("main.create_worker_pod")
    @patch("database.crud.get_job")
    @patch("main.TaskStateController.process_update")
    def test_handle_task_recovery(self, mock_process_update, mock_get_job, mock_pod):
        """
        Verifies that handle_task_recovery sets state to RETRYING 
        and re-spawns a K8s worker [cite: 179-181].
        """
        from main import handle_task_recovery
        
        # Setup a mock task that "failed"
        mock_task = MagicMock(task_id=uuid.uuid4(), job_id=self.job_id)
        mock_process_update.return_value = mock_task # Simulate successful state update
        
        # Run recovery logic
        import asyncio
        asyncio.run(handle_task_recovery(self.db_session, mock_task))

        # Check: Did it transition to RETRYING?
        mock_process_update.assert_any_call(self.db_session, str(mock_task.task_id), db.TaskStatus.RETRYING)
        
        # Check: Did it spawn a NEW pod?
        mock_pod.assert_called_once()
        print("[OK] Recovery Test: Self-healing pod re-spawning verified.")

if __name__ == "__main__":
    unittest.main()