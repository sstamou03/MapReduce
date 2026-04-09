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
        print("[✓] Step 1 Test: Partitioning and Single-Loop Task Ledger verified.")

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
            task.task_type = "MAP"
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
        
        print("[✓] Step 2 Test: Dynamic Pod Spawning verified.")

    def test_healthz(self):
        """Check the Kubernetes Liveness probe."""
        response = client.get("/healthz")
        self.assertEqual(response.status_code, 200)

if __name__ == "__main__":
    unittest.main()