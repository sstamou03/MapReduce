import unittest
import uuid
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

# Mock psycopg2 before importing database logic
import sys
sys.modules['psycopg2'] = MagicMock()

# Import your app and dependencies
from main import app, start_job_orchestration
from database import db,schemas

client = TestClient(app)

class TestManagerStep1(unittest.TestCase):

    @patch("database.crud.get_job")
    @patch("database.crud.update_job_status")
    @patch("database.storage.split_and_upload_input")
    @patch("database.crud.create_task")
    def test_orchestration_flow(self, mock_create_task, mock_split, mock_update_status, mock_get_job):
        """
        Tests that Step 1 orchestration:
        1. Updates job to RUNNING
        2. Calls partitioning logic
        3. Creates the correct number of tasks in DB
        """
        # Setup mocks
        job_id = uuid.uuid4()
        mock_job = MagicMock()
        mock_job.job_id = job_id
        mock_job.input_code_ref = "mapreduce-inputs/test_data"
        mock_get_job.return_value = mock_job
        
        # Simulate 3 partitions being created by storage.py
        mock_split.return_value = ["ref1", "ref2", "ref3"]

        # Run the orchestration logic
        db_session = MagicMock()
        start_job_orchestration(job_id, db_session)

        # ASSERTIONS
        # 1. Check if status was updated to RUNNING
        mock_update_status.assert_called_with(db_session, str(job_id), db.JobStatus.RUNNING)
        
        # 2. Check if partitioning was called with 3 mappers
        mock_split.assert_called_once_with(
            job_id=str(job_id),
            input_ref=mock_job.input_code_ref,
            num_mappers=3
        )

        # 3. Check if 3 tasks were created in the DB
        self.assertEqual(mock_create_task.call_count, 3)
        logger_call_args = mock_create_task.call_args_list[0][1]
        self.assertEqual(logger_call_args['task_type'], db.TaskType.MAP)

    def test_health_endpoint(self):
        """Verify the health check works for Kubernetes Liveness probe."""
        response = client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

if __name__ == "__main__":
    unittest.main()