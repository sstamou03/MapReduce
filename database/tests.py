"""
Unit tests for the MapReduce database layer.
Tests crud.py, schemas.py, and storage.py (split logic).

Run with: python database/tests.py -v

No Docker, PostgreSQL, or MinIO needed - everything runs in-memory!
"""

import sys
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4
from datetime import datetime

# ============================================================
# Mock external modules BEFORE any database imports
# ============================================================
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.extensions"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()
sys.modules["minio"] = MagicMock()
sys.modules["minio.error"] = MagicMock()

# Now safe to import our database modules
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add parent directory to path so we can import database package
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import Base, Job, Task, JobStatus, TaskStatus, TaskType
from database import crud
from database.schemas import (
    JobCreate, JobResponse, JobDetailResponse,
    TaskResponse, TaskStatusUpdate, FileUploadResponse
)
from database import storage


# Use SQLite in-memory for tests instead of PostgreSQL
test_engine = create_engine("sqlite:///:memory:")
TestSession = sessionmaker(bind=test_engine)


# ============================================================
# CRUD TESTS - JOBS
# ============================================================

class TestCrudJobs(unittest.TestCase):
    """Tests for Job CRUD operations in crud.py"""

    def setUp(self):
        Base.metadata.create_all(test_engine)
        self.db = TestSession()

    def tearDown(self):
        self.db.close()
        Base.metadata.drop_all(test_engine)

    def test_create_job(self):
        """Test that create_job creates a job with SUBMITTED status"""
        job = crud.create_job(
            self.db,
            user_id="user123",
            input_code_ref="mapreduce-inputs/job-1/input_data",
            mapper_code_ref="mapreduce-code/job-1/mapper.py",
            reducer_code_ref="mapreduce-code/job-1/reducer.py"
        )
        self.assertIsNotNone(job.job_id)
        self.assertEqual(job.user_id, "user123")
        self.assertEqual(job.status, JobStatus.SUBMITTED)
        self.assertIsNone(job.output_code_ref)

    def test_get_job(self):
        """Test that get_job retrieves the correct job by ID"""
        job = crud.create_job(self.db, "user1", "input_ref", "mapper_ref", "reducer_ref")
        fetched = crud.get_job(self.db, job.job_id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.job_id, job.job_id)
        self.assertEqual(fetched.user_id, "user1")

    def test_get_job_not_found(self):
        """Test that get_job returns None for a non-existent job"""
        result = crud.get_job(self.db, uuid4())
        self.assertIsNone(result)

    def test_get_all_jobs(self):
        """Test that get_all_jobs returns all jobs"""
        crud.create_job(self.db, "user1", "ref1", "ref2", "ref3")
        crud.create_job(self.db, "user2", "ref4", "ref5", "ref6")
        jobs = crud.get_all_jobs(self.db)
        self.assertEqual(len(jobs), 2)

    def test_get_jobs_for_user(self):
        """Test that get_jobs_for_user only returns jobs for the given user"""
        crud.create_job(self.db, "user1", "ref1", "ref2", "ref3")
        crud.create_job(self.db, "user1", "ref4", "ref5", "ref6")
        crud.create_job(self.db, "user2", "ref7", "ref8", "ref9")

        user1_jobs = crud.get_jobs_for_user(self.db, "user1")
        user2_jobs = crud.get_jobs_for_user(self.db, "user2")
        self.assertEqual(len(user1_jobs), 2)
        self.assertEqual(len(user2_jobs), 1)

    def test_update_job_status(self):
        """Test that update_job_status correctly changes the job status"""
        job = crud.create_job(self.db, "user1", "ref1", "ref2", "ref3")
        self.assertEqual(job.status, JobStatus.SUBMITTED)

        updated = crud.update_job_status(self.db, job.job_id, JobStatus.RUNNING)
        self.assertEqual(updated.status, JobStatus.RUNNING)

        completed = crud.update_job_status(self.db, job.job_id, JobStatus.COMPLETED)
        self.assertEqual(completed.status, JobStatus.COMPLETED)

    def test_update_job_output_ref(self):
        """Test that update_job_output_ref saves the output reference"""
        job = crud.create_job(self.db, "user1", "ref1", "ref2", "ref3")
        self.assertIsNone(job.output_code_ref)

        updated = crud.update_job_output_ref(self.db, job.job_id, "mapreduce-outputs/job-1/result")
        self.assertEqual(updated.output_code_ref, "mapreduce-outputs/job-1/result")

    def test_delete_job(self):
        """Test that delete_job removes the job and its tasks"""
        job = crud.create_job(self.db, "user1", "ref1", "ref2", "ref3")
        crud.create_task(self.db, job.job_id, TaskType.MAP, "partition_0")
        crud.create_task(self.db, job.job_id, TaskType.MAP, "partition_1")

        result = crud.delete_job(self.db, job.job_id)
        self.assertTrue(result)
        self.assertIsNone(crud.get_job(self.db, job.job_id))
        self.assertEqual(len(crud.get_tasks_for_job(self.db, job.job_id)), 0)

    def test_delete_job_not_found(self):
        """Test that delete_job returns False for a non-existent job"""
        result = crud.delete_job(self.db, uuid4())
        self.assertFalse(result)


# ============================================================
# CRUD TESTS - TASKS
# ============================================================

class TestCrudTasks(unittest.TestCase):
    """Tests for Task CRUD operations in crud.py"""

    def setUp(self):
        Base.metadata.create_all(test_engine)
        self.db = TestSession()
        self.job = crud.create_job(self.db, "user1", "ref1", "ref2", "ref3")

    def tearDown(self):
        self.db.close()
        Base.metadata.drop_all(test_engine)

    def test_create_task(self):
        """Test that create_task creates a task with PENDING status"""
        task = crud.create_task(self.db, self.job.job_id, TaskType.MAP, "partition_0")
        self.assertIsNotNone(task.task_id)
        self.assertEqual(task.job_id, self.job.job_id)
        self.assertEqual(task.task_type, TaskType.MAP)
        self.assertEqual(task.status, TaskStatus.PENDING)

    def test_get_tasks_for_job(self):
        """Test getting all tasks for a specific job"""
        crud.create_task(self.db, self.job.job_id, TaskType.MAP, "p0")
        crud.create_task(self.db, self.job.job_id, TaskType.MAP, "p1")
        crud.create_task(self.db, self.job.job_id, TaskType.REDUCE, "p2")

        all_tasks = crud.get_tasks_for_job(self.db, self.job.job_id)
        self.assertEqual(len(all_tasks), 3)

    def test_get_tasks_filtered_by_type(self):
        """Test filtering tasks by MAP or REDUCE type"""
        crud.create_task(self.db, self.job.job_id, TaskType.MAP, "p0")
        crud.create_task(self.db, self.job.job_id, TaskType.MAP, "p1")
        crud.create_task(self.db, self.job.job_id, TaskType.REDUCE, "p2")

        map_tasks = crud.get_tasks_for_job(self.db, self.job.job_id, TaskType.MAP)
        reduce_tasks = crud.get_tasks_for_job(self.db, self.job.job_id, TaskType.REDUCE)
        self.assertEqual(len(map_tasks), 2)
        self.assertEqual(len(reduce_tasks), 1)

    def test_update_task_status_running(self):
        """Test updating a task to RUNNING with worker pod ID"""
        task = crud.create_task(self.db, self.job.job_id, TaskType.MAP, "p0")

        updated = crud.update_task_status(
            self.db, task.task_id, TaskStatus.RUNNING, worker_pod_id="worker-pod-abc"
        )
        self.assertEqual(updated.status, TaskStatus.RUNNING)
        self.assertEqual(updated.worker_pod_id, "worker-pod-abc")

    def test_update_task_status_completed(self):
        """Test completing a task with output reference"""
        task = crud.create_task(self.db, self.job.job_id, TaskType.MAP, "p0")

        completed = crud.update_task_status(
            self.db, task.task_id, TaskStatus.COMPLETED,
            output_partition_ref="mapreduce-intermediates/job-1/task-1_output"
        )
        self.assertEqual(completed.status, TaskStatus.COMPLETED)
        self.assertEqual(completed.output_partition_ref, "mapreduce-intermediates/job-1/task-1_output")

    def test_update_task_status_failed_and_retry(self):
        """Test the failure recovery flow: RUNNING -> FAILED -> RETRYING"""
        task = crud.create_task(self.db, self.job.job_id, TaskType.MAP, "p0")

        failed = crud.update_task_status(self.db, task.task_id, TaskStatus.FAILED)
        self.assertEqual(failed.status, TaskStatus.FAILED)

        retrying = crud.update_task_status(
            self.db, task.task_id, TaskStatus.RETRYING, worker_pod_id="new-worker-pod"
        )
        self.assertEqual(retrying.status, TaskStatus.RETRYING)
        self.assertEqual(retrying.worker_pod_id, "new-worker-pod")


# ============================================================
# SCHEMA TESTS
# ============================================================

class TestSchemas(unittest.TestCase):
    """Tests for Pydantic schemas in schemas.py"""

    def test_job_create_valid(self):
        """Test that JobCreate accepts valid data"""
        schema = JobCreate(
            input_code_ref="mapreduce-inputs/job-1/input_data",
            mapper_code_ref="mapreduce-code/job-1/mapper.py",
            reducer_code_ref="mapreduce-code/job-1/reducer.py"
        )
        self.assertEqual(schema.input_code_ref, "mapreduce-inputs/job-1/input_data")

    def test_job_create_missing_field(self):
        """Test that JobCreate rejects data with missing required fields"""
        with self.assertRaises(Exception):
            JobCreate(
                input_code_ref="ref1",
                mapper_code_ref="ref2"
                # missing reducer_code_ref!
            )

    def test_job_response_from_dict(self):
        """Test that JobResponse correctly parses a dictionary"""
        data = {
            "job_id": uuid4(),
            "user_id": "user123",
            "status": "SUBMITTED",
            "input_code_ref": "ref1",
            "mapper_code_ref": "ref2",
            "reducer_code_ref": "ref3",
            "output_code_ref": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        schema = JobResponse(**data)
        self.assertEqual(schema.user_id, "user123")
        self.assertEqual(schema.status, "SUBMITTED")

    def test_task_status_update_valid(self):
        """Test that TaskStatusUpdate accepts valid worker report"""
        update = TaskStatusUpdate(
            task_id=uuid4(),
            status="COMPLETED",
            worker_pod_id="worker-pod-123",
            output_partition_ref="mapreduce-intermediates/job-1/output"
        )
        self.assertEqual(update.status, "COMPLETED")

    def test_task_status_update_minimal(self):
        """Test TaskStatusUpdate works with only required fields"""
        update = TaskStatusUpdate(task_id=uuid4(), status="FAILED")
        self.assertEqual(update.status, "FAILED")
        self.assertIsNone(update.worker_pod_id)

    def test_file_upload_response(self):
        """Test FileUploadResponse with partial refs"""
        resp = FileUploadResponse(
            input_ref="mapreduce-inputs/job-1/input_data",
            mapper_ref="mapreduce-code/job-1/mapper.py"
        )
        self.assertEqual(resp.input_ref, "mapreduce-inputs/job-1/input_data")
        self.assertIsNone(resp.reducer_ref)


# ============================================================
# SPLIT LOGIC TESTS
# ============================================================

class TestSplitLogic(unittest.TestCase):
    """Tests for the input splitting logic in storage.py (with mocked MinIO)"""

    @patch.object(storage, "get_data_from_ref")
    @patch.object(storage, "upload_data_bytes")
    def test_split_even(self, mock_upload, mock_download):
        """Test splitting 6 lines evenly into 3 chunks of 2 lines each"""
        mock_download.return_value = b"line1\nline2\nline3\nline4\nline5\nline6\n"
        mock_upload.side_effect = lambda bucket, obj, data: f"{bucket}/{obj}"

        refs = storage.split_and_upload_input("job-123", "mapreduce-inputs/job-123/input_data", 3)
        self.assertEqual(len(refs), 3)
        self.assertEqual(mock_upload.call_count, 3)

    @patch.object(storage, "get_data_from_ref")
    @patch.object(storage, "upload_data_bytes")
    def test_split_uneven(self, mock_upload, mock_download):
        """Test splitting 7 lines into 3 chunks (3+2+2 distribution)"""
        mock_download.return_value = b"a\nb\nc\nd\ne\nf\ng\n"
        mock_upload.side_effect = lambda bucket, obj, data: f"{bucket}/{obj}"

        refs = storage.split_and_upload_input("job-456", "ref", 3)
        self.assertEqual(len(refs), 3)

        calls = mock_upload.call_args_list
        chunk0_data = calls[0][0][2]
        chunk1_data = calls[1][0][2]
        chunk2_data = calls[2][0][2]

        self.assertEqual(chunk0_data.count(b"\n"), 3)
        self.assertEqual(chunk1_data.count(b"\n"), 2)
        self.assertEqual(chunk2_data.count(b"\n"), 2)

    @patch.object(storage, "get_data_from_ref")
    @patch.object(storage, "upload_data_bytes")
    def test_split_single_mapper(self, mock_upload, mock_download):
        """Test splitting with 1 mapper - all data stays in one chunk"""
        mock_download.return_value = b"line1\nline2\nline3\n"
        mock_upload.side_effect = lambda bucket, obj, data: f"{bucket}/{obj}"

        refs = storage.split_and_upload_input("job-789", "ref", 1)
        self.assertEqual(len(refs), 1)

        chunk_data = mock_upload.call_args_list[0][0][2]
        self.assertEqual(chunk_data, b"line1\nline2\nline3\n")


if __name__ == "__main__":
    unittest.main()
