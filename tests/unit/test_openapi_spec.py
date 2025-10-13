"""
Tests to validate that all API endpoints have proper OpenAPI specification including:
- response_model
- status_code  
- responses
- description
- examples
"""
import json
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

# Mock all the dependencies that require external services
mock_modules = [
    'workerfacing_api.dependencies',
    'workerfacing_api.settings', 
    'workerfacing_api.core.filesystem',
    'workerfacing_api.core.queue',
    'workerfacing_api.crud',
    'fastapi_utils.tasks',
    'sqlalchemy',
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Mock repeat_every decorator
def mock_repeat_every(*args, **kwargs):
    def decorator(func):
        return func
    return decorator

sys.modules['fastapi_utils.tasks'].repeat_every = mock_repeat_every

# Now import the app
from workerfacing_api.main import workerfacing_app


def test_openapi_schema_structure():
    """Test that the OpenAPI schema contains expected fields for all endpoints.""" 
    # Mock environment variables
    with patch.dict(os.environ, {
        'COGNITO_USER_POOL_ID': 'test-pool',
        'COGNITO_CLIENT_ID': 'test-client',
        'COGNITO_REGION': 'us-east-1'
    }):
        with TestClient(workerfacing_app) as client:
            response = client.get("/openapi.json")
            assert response.status_code == 200
            
            openapi_spec = response.json()
            paths = openapi_spec["paths"]
            
            # Expected paths from the endpoints
            expected_paths = [
                "/",
                "/access_info", 
                "/files/{file_id}/download",
                "/files/{file_id}/url",
                "/jobs",
                "/jobs/{job_id}/status",
                "/jobs/{job_id}/files/upload",
                "/jobs/{job_id}/files/url",
                "/_jobs"
            ]
            
            for path in expected_paths:
                assert path in paths, f"Path {path} not found in OpenAPI spec"


def test_endpoints_have_descriptions():
    """Test that all endpoints have descriptions."""
    with patch.dict(os.environ, {
        'COGNITO_USER_POOL_ID': 'test-pool',
        'COGNITO_CLIENT_ID': 'test-client',
        'COGNITO_REGION': 'us-east-1'
    }):
        with TestClient(workerfacing_app) as client:
            response = client.get("/openapi.json")
            openapi_spec = response.json()
            paths = openapi_spec["paths"]
            
            for path, methods in paths.items():
                for method, spec in methods.items():
                    assert "description" in spec, f"Endpoint {method.upper()} {path} missing description"
                    assert spec["description"], f"Endpoint {method.upper()} {path} has empty description"


def test_endpoints_have_responses():
    """Test that all endpoints have responses defined."""
    with patch.dict(os.environ, {
        'COGNITO_USER_POOL_ID': 'test-pool',
        'COGNITO_CLIENT_ID': 'test-client',
        'COGNITO_REGION': 'us-east-1'
    }):
        with TestClient(workerfacing_app) as client:
            response = client.get("/openapi.json")
            openapi_spec = response.json()
            paths = openapi_spec["paths"]
            
            for path, methods in paths.items():
                for method, spec in methods.items():
                    assert "responses" in spec, f"Endpoint {method.upper()} {path} missing responses"
                    responses = spec["responses"]
                    assert len(responses) > 0, f"Endpoint {method.upper()} {path} has no response codes defined"
                    
                    # Check for success status codes
                    success_codes = [code for code in responses.keys() if code.startswith("2")]
                    assert len(success_codes) > 0, f"Endpoint {method.upper()} {path} has no success response codes"


def test_schemas_have_examples():
    """Test that key schemas have examples for OpenAPI documentation."""
    with patch.dict(os.environ, {
        'COGNITO_USER_POOL_ID': 'test-pool', 
        'COGNITO_CLIENT_ID': 'test-client',
        'COGNITO_REGION': 'us-east-1'
    }):
        with TestClient(workerfacing_app) as client:
            response = client.get("/openapi.json")
            openapi_spec = response.json()
            components = openapi_spec.get("components", {})
            schemas = components.get("schemas", {})
            
            # Key schemas that should have examples
            schema_examples_to_check = [
                "FileHTTPRequest",
                "HardwareSpecs", 
                "MetaSpecs",
                "AppSpecs",
                "HandlerSpecs",
                "PathsUploadSpecs",
                "SubmittedJob",
                "WelcomeMessage"
            ]
            
            for schema_name in schema_examples_to_check:
                if schema_name in schemas:
                    schema = schemas[schema_name]
                    properties = schema.get("properties", {})
                    
                    # Check if any properties have examples
                    has_examples = any("example" in prop for prop in properties.values())
                    assert has_examples, f"Schema {schema_name} should have examples in its properties"


def test_root_endpoint_specification():
    """Test that the root endpoint has proper API specification."""
    with patch.dict(os.environ, {
        'COGNITO_USER_POOL_ID': 'test-pool',
        'COGNITO_CLIENT_ID': 'test-client', 
        'COGNITO_REGION': 'us-east-1'
    }):
        with TestClient(workerfacing_app) as client:
            response = client.get("/openapi.json")
            openapi_spec = response.json()
            
            root_spec = openapi_spec["paths"]["/"]["get"]
            
            # Check all required fields are present
            assert "description" in root_spec
            assert "responses" in root_spec
            assert "200" in root_spec["responses"]
            assert "tags" in root_spec
            
            # Test the actual endpoint response
            root_response = client.get("/")
            assert root_response.status_code == 200
            data = root_response.json()
            assert "message" in data
            assert data["message"] == "Welcome to the DECODE OpenCloud Worker-facing API"


if __name__ == "__main__":
    # Can run directly for quick testing
    test_openapi_schema_structure()
    test_endpoints_have_descriptions()
    test_endpoints_have_responses()
    test_schemas_have_examples()
    test_root_endpoint_specification()
    print("All OpenAPI specification tests passed!")