import unittest
import os
import sys
import json
import tempfile
from unittest.mock import patch, MagicMock

# Mock psycopg2 before importing anything from database
sys.modules['psycopg2'] = MagicMock()

# Add project root to path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from worker import load_user_module, report_status

"""
Unit tests for the Worker Service.
Tests the dynamic code loading, MAP/REDUCE execution, and status reporting.

Run with: python worker-service/tests.py -v
"""


# =============================================
# Test 1: Dynamic Code Loading
# =============================================

class TestLoadUserModule(unittest.TestCase):

    def test_load_map_function(self):
        """Test that load_user_module can load a .py file with a map() function."""
        # Create a temporary Python file with a simple map function
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('def map(data):\n    return {"hello": 1}\n')
            temp_path = f.name

        try:
            module = load_user_module(temp_path)
            self.assertTrue(hasattr(module, 'map'))
            result = module.map("some data")
            self.assertEqual(result, {"hello": 1})
        finally:
            os.unlink(temp_path)

    def test_load_reduce_function(self):
        """Test that load_user_module can load a .py file with a reduce() function."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('def reduce(data):\n    return {"total": 42}\n')
            temp_path = f.name

        try:
            module = load_user_module(temp_path)
            self.assertTrue(hasattr(module, 'reduce'))
            result = module.reduce("some data")
            self.assertEqual(result, {"total": 42})
        finally:
            os.unlink(temp_path)

    def test_load_missing_function(self):
        """Test that a script without map() wont have the attribute."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('def something_else():\n    pass\n')
            temp_path = f.name

        try:
            module = load_user_module(temp_path)
            self.assertFalse(hasattr(module, 'map'))
            self.assertFalse(hasattr(module, 'reduce'))
        finally:
            os.unlink(temp_path)


# =============================================
# Test 2: Word Count MAP function
# =============================================

class TestMapExecution(unittest.TestCase):

    def test_word_count_mapper(self):
        """Test a realistic word count mapper on sample text."""
        # Write a simple word count mapper
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(
                'def map(data):\n'
                '    counts = {}\n'
                '    for word in data.split():\n'
                '        counts[word] = counts.get(word, 0) + 1\n'
                '    return counts\n'
            )
            temp_path = f.name

        try:
            module = load_user_module(temp_path)
            result = module.map("hello world hello")
            self.assertEqual(result["hello"], 2)
            self.assertEqual(result["world"], 1)
        finally:
            os.unlink(temp_path)

    def test_result_is_json_serializable(self):
        """Test that the map result can be serialized to JSON (needed for MinIO upload)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('def map(data):\n    return {"key": "value", "count": 5}\n')
            temp_path = f.name

        try:
            module = load_user_module(temp_path)
            result = module.map("test")
            # This is exactly what worker.py does before uploading
            result_bytes = json.dumps(result).encode("utf-8")
            self.assertIsInstance(result_bytes, bytes)
            # Verify we can decode it back
            decoded = json.loads(result_bytes.decode("utf-8"))
            self.assertEqual(decoded["count"], 5)
        finally:
            os.unlink(temp_path)


# =============================================
# Test 3: Report Status to Manager
# =============================================

class TestReportStatus(unittest.TestCase):

    @patch('worker.requests.post')
    def test_report_running(self, mock_post):
        """Test that report_status sends correct payload for RUNNING."""
        mock_post.return_value = MagicMock(status_code=200)

        report_status("task-123", "RUNNING")

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        self.assertEqual(payload['task_id'], "task-123")
        self.assertEqual(payload['status'], "RUNNING")
        self.assertNotIn('output_partition_ref', payload)

    @patch('worker.requests.post')
    def test_report_completed_with_output(self, mock_post):
        """Test that report_status includes output_ref when COMPLETED."""
        mock_post.return_value = MagicMock(status_code=200)

        report_status("task-456", "COMPLETED", output_ref="mapreduce-intermediates/job-1/output")

        payload = mock_post.call_args[1]['json']
        self.assertEqual(payload['status'], "COMPLETED")
        self.assertEqual(payload['output_partition_ref'], "mapreduce-intermediates/job-1/output")

    @patch('worker.requests.post')
    def test_report_failed(self, mock_post):
        """Test that report_status sends FAILED without crashing."""
        mock_post.return_value = MagicMock(status_code=200)

        report_status("task-789", "FAILED")

        payload = mock_post.call_args[1]['json']
        self.assertEqual(payload['status'], "FAILED")

    @patch('worker.requests.post')
    def test_manager_unreachable(self, mock_post):
        """Test that the worker doesnt crash if the Manager is unreachable."""
        mock_post.side_effect = Exception("Connection refused")

        # This should NOT raise an exception
        report_status("task-000", "RUNNING")


if __name__ == "__main__":
    unittest.main()
