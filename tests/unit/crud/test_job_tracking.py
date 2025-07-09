"""Tests for job_tracking module."""

from unittest.mock import MagicMock, patch

import pytest

from workerfacing_api.crud.job_tracking import update_job
from workerfacing_api.exceptions import JobDeletedException
from workerfacing_api.schemas.rds_models import JobStates


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
    
    assert f"Job {job_id} not found; it was probably deleted by the user." in str(exc_info.value)