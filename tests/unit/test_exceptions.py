"""Tests for JobDeletedException exception handling."""

import pytest
from unittest.mock import patch, MagicMock

from workerfacing_api.exceptions import JobDeletedException
from workerfacing_api.crud.job_tracking import update_job
from workerfacing_api.schemas.rds_models import JobStates


def test_job_deleted_exception_creation():
    """Test that JobDeletedException is created correctly."""
    job_id = 123
    message = "test message"
    
    exc = JobDeletedException(job_id, message)
    
    assert exc.job_id == job_id
    assert f"Job {job_id} not found; {message}" in str(exc)


def test_job_deleted_exception_default_message():
    """Test that JobDeletedException uses default message."""
    job_id = 456
    
    exc = JobDeletedException(job_id)
    
    assert exc.job_id == job_id
    assert "Job 456 not found; Job was probably deleted by the user" in str(exc)


@patch('workerfacing_api.crud.job_tracking.requests.put')
def test_update_job_raises_job_deleted_exception(mock_put):
    """Test that update_job raises JobDeletedException on 404 response."""
    # Mock a 404 response
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_put.return_value = mock_response
    
    job_id = 789
    
    with pytest.raises(JobDeletedException) as exc_info:
        update_job(job_id, JobStates.running)
    
    assert exc_info.value.job_id == job_id
    assert "it was probably deleted by the user" in str(exc_info.value)


@patch('workerfacing_api.crud.job_tracking.requests.put')
def test_update_job_success_no_exception(mock_put):
    """Test that update_job doesn't raise exception on success."""
    # Mock a successful response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_put.return_value = mock_response
    
    job_id = 999
    
    # This should not raise an exception
    update_job(job_id, JobStates.running)
    
    # Verify the request was made
    mock_put.assert_called_once()