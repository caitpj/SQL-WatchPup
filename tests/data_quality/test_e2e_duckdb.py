import os
import subprocess
import sys
import pytest
import re
from pathlib import Path

def test_data_quality_pipeline():
    """
    Test the data quality pipeline by:
    1. Running mock_db.py to create test database
    2. Running run_dq_checks.py to perform data quality checks
    3. Verifying the expected test results
    """
    # Get the directory of the current test file
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    
    # Paths to the scripts
    mock_db_script = current_dir / "mock_db.py"
    run_dq_checks_script = current_dir / "../../data_quality/run_dq_checks.py"
    
    # Check that both scripts exist
    assert mock_db_script.exists(), f"Mock DB script not found at {mock_db_script}"
    assert run_dq_checks_script.exists(), f"DQ checks script not found at {run_dq_checks_script}"
    
    # Step 1: Run the mock_db.py script to create the test database
    print("\n=== Running mock_db.py to create test database ===")
    mock_db_result = subprocess.run(
        [sys.executable, str(mock_db_script)],
        capture_output=True,
        text=True
    )
    
    # Check that the mock database was created successfully
    assert mock_db_result.returncode == 0, f"Mock DB creation failed with: {mock_db_result.stderr}"
    print(mock_db_result.stdout)
    
    # Verify the database file was created
    db_file = current_dir / "data_quality_test.duckdb"
    assert db_file.exists(), f"Database file was not created at {db_file}"
    
    # Step 2: Run the run_dq_checks.py script to perform data quality checks with config path
    print("\n=== Running run_dq_checks.py to perform data quality checks ===")
    config_file = current_dir / "config.yml"
    
    # Make sure the config file exists
    assert config_file.exists(), f"Config file not found at {config_file}"
    
    dq_check_result = subprocess.run(
        [sys.executable, str(run_dq_checks_script), "--config", str(config_file)],
        capture_output=True,
        text=True
    )
    
    # Check that the data quality checks ran successfully
    assert dq_check_result.returncode == 0, f"Data quality check failed with: {dq_check_result.stderr}"
    
    # Store the output for comparison
    actual_output = dq_check_result.stdout
    print(actual_output)
    
    # Define the expected results
    expected_results = [
        # watchpup.cheese
        ("Running tests for watchpup.cheese", True),
        ("Column: id", True),
        ("Test 'unique': PASS", True),
        ("Test 'no_nulls': PASS", True),
        ("Test 'max_len: 20': FAIL", True),
        ("Column: status", True),
        ("Test 'no_nulls': FAIL", True),
        ("Test 'accepted_values: ['old', 'fresh', 'pending']': FAIL", True),
        
        # watchpup.chess
        ("Running tests for watchpup.chess", True),
        ("Column: game_id", True),
        ("Test 'unique': FAIL", True),
        ("Test 'no_nulls': FAIL", True),
        ("Column: date", True),
        ("Test 'no_nulls': FAIL", True),
        ("Test 'no_future_dates': FAIL", True),
        
        # watchpup.users
        ("Running tests for watchpup.users", True),
        ("Column: user_id", True),
        ("Test 'unique': PASS", True),
        ("Test 'no_nulls': PASS", True),
        ("Test 'max_len: 20': FAIL", True),
        ("Column: username", True),
        ("Test 'no_nulls': FAIL", True),
        ("Test 'unique': FAIL", True),
        ("Test 'max_len: 20': PASS", True),
        ("Column: status", True),
        ("Test 'no_nulls': PASS", True),
        ("Test 'accepted_values: ['active', 'inactive', 'pending', 'suspended']': FAIL", True),
        ("Column: last_login", True),
        ("Test 'no_nulls': PASS", True),
        ("Test 'no_future_dates': FAIL", True),
        
        # watchpup.orders
        ("Running tests for watchpup.orders", True),
        ("Column: order_id", True),
        ("Test 'unique': FAIL", True),
        ("Test 'no_nulls': PASS", True),
        ("Column: order_date", True),
        ("Test 'no_nulls': PASS", True),
        ("Test 'no_future_dates': FAIL", True),
        ("Column: amount", True),
        ("Test 'no_nulls': FAIL", True),
        ("Test 'positive_amount': FAIL", True),
        
        # watchpup.currencies
        ("Running tests for watchpup.currencies", True),
        ("Column: exchange_from_ccy", True),
        ("Test 'no_nulls': FAIL", True),
        ("Column: exchange_to_ccy", True),
        ("Test 'no_nulls': FAIL", True),
        ("Column: reporting_date", True),
        ("Test 'no_nulls': FAIL", True),
        ("Test 'no_future_dates': FAIL", True),
        ("Column: exchange_rate", True),
        ("Test 'no_nulls': FAIL", True),
        ("Test 'positive_amount': FAIL", True),
    ]
    
    # Check that each expected result is in the output
    for expected_text, should_exist in expected_results:
        # Use regex to handle potential color codes and formatting
        pattern = re.escape(expected_text).replace("'", "['']")
        found = re.search(pattern, actual_output, re.MULTILINE)
        
        if should_exist:
            assert found, f"Expected text not found in output: {expected_text}"
        else:
            assert not found, f"Unexpected text found in output: {expected_text}"
    
    print("\n=== Data quality tests completed successfully ===")