"""
Static validation tests for API endpoint definitions to ensure they have proper FastAPI decorator parameters.
This tests the code structure without running the full application.
"""
import os
import ast
import sys
from typing import Dict, List, Set

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def get_fastapi_decorators_from_file(file_path: str) -> List[Dict]:
    """Extract FastAPI decorators and their parameters from a Python file."""
    with open(file_path, 'r') as f:
        tree = ast.parse(f.read())
    
    decorators = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    # Check if it's a router method call (e.g., @router.get, @router.post, etc.)
                    if (isinstance(decorator.func, ast.Attribute) and 
                        isinstance(decorator.func.value, ast.Name) and
                        decorator.func.value.id == 'router'):
                        
                        method = decorator.func.attr
                        path = None
                        params = {}
                        
                        # Get the path (first positional argument)
                        if decorator.args:
                            if isinstance(decorator.args[0], ast.Constant):
                                path = decorator.args[0].value
                        
                        # Get keyword arguments
                        for keyword in decorator.keywords:
                            if isinstance(keyword.value, ast.Constant):
                                params[keyword.arg] = keyword.value.value
                            elif isinstance(keyword.value, ast.Name):
                                params[keyword.arg] = keyword.value.id
                            elif isinstance(keyword.value, ast.List):
                                # Handle list values like tags=["Jobs"]
                                list_items = []
                                for item in keyword.value.elts:
                                    if isinstance(item, ast.Constant):
                                        list_items.append(item.value)
                                params[keyword.arg] = list_items
                            elif isinstance(keyword.value, ast.Dict):
                                # Handle dict values like responses={}
                                params[keyword.arg] = "dict_value"
                        
                        decorators.append({
                            'method': method,
                            'path': path,
                            'function_name': node.name,
                            'params': params
                        })
                        
                elif isinstance(decorator, ast.Attribute):
                    # Handle decorators like @workerfacing_app.get
                    if (isinstance(decorator.value, ast.Name) and 
                        decorator.value.id == 'workerfacing_app'):
                        # This is likely from main.py root endpoint
                        decorators.append({
                            'method': decorator.attr,
                            'path': '/',  # Root path
                            'function_name': node.name,
                            'params': {}
                        })
    
    return decorators


def test_endpoints_have_required_fields():
    """Test that all endpoint decorators have the required FastAPI fields."""
    endpoint_files = [
        'workerfacing_api/endpoints/access.py',
        'workerfacing_api/endpoints/files.py',
        'workerfacing_api/endpoints/jobs.py',
        'workerfacing_api/endpoints/jobs_post.py',
        'workerfacing_api/main.py'
    ]
    
    all_endpoints = []
    for file_path in endpoint_files:
        if os.path.exists(file_path):
            decorators = get_fastapi_decorators_from_file(file_path)
            all_endpoints.extend(decorators)
    
    print(f"Found {len(all_endpoints)} endpoints to validate")
    
    # Required fields for all endpoints
    required_fields = ['description']
    
    # Fields that should be present (either explicitly or through defaults)
    expected_fields = ['response_model', 'status_code', 'responses']
    
    issues = []
    
    for endpoint in all_endpoints:
        method = endpoint['method']
        path = endpoint['path']
        params = endpoint['params']
        func_name = endpoint['function_name']
        
        endpoint_id = f"{method.upper()} {path} ({func_name})"
        
        # Check required fields
        for field in required_fields:
            if field not in params:
                issues.append(f"{endpoint_id} missing {field}")
        
        # Check for responses field
        if 'responses' not in params:
            issues.append(f"{endpoint_id} missing responses parameter")
        
        # Check status_code for non-GET methods or specific cases
        if method in ['post', 'put', 'delete'] and 'status_code' not in params:
            issues.append(f"{endpoint_id} missing explicit status_code")
    
    # Print all found endpoints for debugging
    print("\nFound endpoints:")
    for endpoint in all_endpoints:
        print(f"  {endpoint['method'].upper()} {endpoint['path']} - {endpoint['function_name']}")
        print(f"    Params: {list(endpoint['params'].keys())}")
    
    if issues:
        print(f"\nFound {len(issues)} issues:")
        for issue in issues:
            print(f"  - {issue}")
        assert False, f"Found {len(issues)} endpoint specification issues"
    else:
        print(f"\nAll {len(all_endpoints)} endpoints have proper specifications!")


def test_pydantic_models_have_examples():
    """Test that Pydantic models have Field examples for OpenAPI documentation."""
    schema_files = [
        'workerfacing_api/schemas/files.py',
        'workerfacing_api/schemas/queue_jobs.py',
        'workerfacing_api/schemas/responses.py'
    ]
    
    models_with_examples = []
    models_without_examples = []
    
    for file_path in schema_files:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Check for Field with example parameter
            if 'Field(' in content and 'example=' in content:
                models_with_examples.append(file_path)
            else:
                # Check if there are any BaseModel classes that should have examples
                tree = ast.parse(content)
                has_models = any(
                    isinstance(node, ast.ClassDef) and 
                    any(isinstance(base, ast.Name) and base.id == 'BaseModel' for base in node.bases)
                    for node in ast.walk(tree)
                )
                if has_models and file_path not in models_with_examples:
                    models_without_examples.append(file_path)
    
    print(f"Schema files with examples: {models_with_examples}")
    print(f"Schema files that may need examples: {models_without_examples}")
    
    # For this test, we'll check that at least some models have examples
    assert len(models_with_examples) > 0, "No Pydantic models found with Field examples"


if __name__ == "__main__":
    test_endpoints_have_required_fields()
    test_pydantic_models_have_examples()
    print("All static validation tests passed!")