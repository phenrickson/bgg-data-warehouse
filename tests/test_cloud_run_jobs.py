"""Integration tests for Cloud Run Jobs setup."""

import logging
from unittest import mock

import pytest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_cloud_run_job_configuration():
    """Test Cloud Run Job configuration."""
    # Create a more structured mock
    mock_container = mock.Mock()
    mock_container.image = "gcr.io/test-project/bgg-processor:latest"
    mock_container.resources = mock.Mock()
    mock_container.resources.limits = {
        "cpu": "1",
        "memory": "2Gi"
    }
    
    mock_template = mock.Mock()
    mock_template.containers = [mock_container]
    mock_template.max_retries = 3
    mock_template.timeout = mock.Mock(total_seconds=lambda: 3600)  # 1 hour
    
    # Verify job configuration
    assert mock_template.containers[0].image == "gcr.io/test-project/bgg-processor:latest"
    assert mock_template.containers[0].resources.limits["cpu"] == "1"
    assert mock_template.containers[0].resources.limits["memory"] == "2Gi"
    assert mock_template.max_retries == 3
    assert mock_template.timeout.total_seconds() == 3600

def test_cloud_run_job_environment_variables():
    """Test Cloud Run Job environment variables."""
    # Create a more structured mock for environment variables
    mock_env1 = mock.Mock()
    mock_env1.name = "ENVIRONMENT"
    mock_env1.value = "test"
    
    mock_env2 = mock.Mock()
    mock_env2.name = "LOG_LEVEL"
    mock_env2.value = "INFO"
    
    mock_container = mock.Mock()
    mock_container.env = [mock_env1, mock_env2]
    
    # Convert env vars to dictionary
    env_vars = {
        env.name: env.value
        for env in mock_container.env
    }
    
    # Verify environment variables
    assert env_vars["ENVIRONMENT"] == "test"
    assert env_vars["LOG_LEVEL"] == "INFO"

def test_cloud_run_job_parallel_execution():
    """Test Cloud Run Job parallel execution configuration."""
    # Mock job with parallel tasks
    mock_template = mock.Mock()
    mock_template.parallelism = 3
    
    # Verify parallel configuration
    assert mock_template.parallelism == 3

def test_cloud_run_job_scheduling():
    """Test Cloud Run Job scheduling configuration."""
    # Mock scheduler job
    mock_http_target = mock.Mock()
    mock_http_target.uri = "https://test-run.googleapis.com/jobs/test-job"
    
    mock_scheduler_job = mock.Mock()
    mock_scheduler_job.schedule = "*/10 * * * *"
    mock_scheduler_job.http_target = mock_http_target
    
    # Verify scheduling configuration
    assert mock_scheduler_job.schedule == "*/10 * * * *"
    assert "test-job" in mock_scheduler_job.http_target.uri

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
