import unittest
import uuid
import sys
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

# Mock psycopg2 before importing database logic to avoid connection errors
sys.modules['psycopg2'] = MagicMock()

from main import app, start_job_orchestration
from database import db

client = TestClient(app)

class TestManagerOrchestration(unittest.TestCase):

    def setUp(self):
        """Prepare common test data."""
        self.job_id = uuid.uuid4()
        self.db_session = MagicMock()

    
    @patch("database.crud.get_job")
    @patch("database.crud.update_job_status")
    @patch("database.storage.split_and_upload_input")
    @patch("database.crud.create_task")
    @patch("main.create_worker_pod") 
    def test_step1_partitioning_logic(self, mock_pod, mock_create_task, mock_split, mock_update_status, mock_get_job):
        """Verify Step 1: Status updates, partitioning, and task creation."""
        mock_job = MagicMock()
        mock_job.job_id = self.job_id
        mock_get_job.return_value = mock_job
        mock_split.return_value = ["ref0", "ref1", "ref2"]

        start_job_orchestration(self.job_id, self.db_session)

        # Confirm DB was updated correctly 
        mock_update_status.assert_called_with(self.db_session, str(self.job_id), db.JobStatus.RUNNING)
        self.assertEqual(mock_create_task.call_count, 3)
        print("\n[✓] Test Passed: Partitioning and DB Tasks verified.")

    
    @patch("main.k8s_batch_v1")
    @patch("database.crud.get_job")
    @patch("database.crud.update_job_status")
    @patch("database.storage.split_and_upload_input")
    @patch("database.crud.create_task")
    def test_step2_k8s_job_spawning(self, mock_create_task, mock_split, mock_update_status, mock_get_job, mock_k8s):
        """Verify Step 2: Manager actually requests Pods from Kubernetes."""
        mock_job = MagicMock()
        mock_job.job_id = self.job_id
        mock_job.mapper_code_ref = "minio/mapper.py"
        mock_get_job.return_value = mock_job
        mock_split.return_value = ["ref0", "ref1", "ref2"]
        
        # Ensure task objects have valid IDs for the pod name 
        def side_effect_create_task(*args, **kwargs):
            task = MagicMock()
            task.task_id = uuid.uuid4()
            task.task_type = "MAP"
            task.input_partition_ref = kwargs.get('input_partition_ref', "ref")
            return task
        mock_create_task.side_effect = side_effect_create_task

        start_job_orchestration(self.job_id, self.db_session)

        # Verify K8s API was called once per task 
        self.assertEqual(mock_k8s.create_namespaced_job.call_count, 3)
        
        # Peek at the environment variables sent to K8s
        job_body = mock_k8s.create_namespaced_job.call_args_list[0][1]['body']
        env_vars = job_body.spec.template.spec.containers[0].env
        task_type_env = next(e for e in env_vars if e.name == "TASK_TYPE")
        
        self.assertEqual(task_type_env.value, "MAP")
        print("[✓] Test Passed: K8s Dynamic Spawning verified.")

    def test_health_check(self):
        """Verify Liveness probe for K8s."""
        response = client.get("/healthz")
        self.assertEqual(response.status_code, 200)

if __name__ == "__main__":
    unittest.main()