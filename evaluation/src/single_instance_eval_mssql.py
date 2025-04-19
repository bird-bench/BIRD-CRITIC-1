#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evaluation script for a single SQL instance (MS SQL Server version)
This is designed to be called from wrapper_evaluation_mssql.py
Enhanced with better error handling and cleanup procedures.
"""

import argparse
import json
import sys
import os
import io
import traceback
import time
import gc
from datetime import date

# Local imports
from logger import configure_logger, NullLogger
from mssql_utils import (
    perform_query_on_sqlserver_databases,
    close_sqlserver_connection,
    execute_queries,
    get_connection_for_phase,
    reset_and_restore_database,
    load_jsonl,
    split_field,
    run_preprocessing,
)
from mssql_test_utils import (
    check_sql_function_usage,
    remove_distinct,
    preprocess_results,
    preprocess_results_dict,
    performance_compare_by_qep,
    ex_base,
    ex_base_dict,
)

from datetime import date


def run_test_case(test_code, result, logger, conn, issue_sql, sol_sql, db_name):
    """
    Execute a single test case, capturing AssertionError or other exceptions.
    Returns True if test passed, False otherwise, and an error message.
    """
    global_env = {
        "perform_query_on_sqlserver_databases": perform_query_on_sqlserver_databases,
        "execute_queries": execute_queries,
        "ex_base": ex_base,
        "ex_base_dict": ex_base_dict,
        "performance_compare_by_qep": performance_compare_by_qep,
        "check_sql_function_usage": check_sql_function_usage,
        "remove_distinct": remove_distinct,
        "preprocess_results": preprocess_results,
        "preprocess_results_dict": preprocess_results_dict,
        "pred_query_result": result,
        "date": date,
    }
    local_env = {
        "conn": conn,
        "pred_sqls": issue_sql,
        "sol_sqls": sol_sql,
        "db_name": db_name,
    }

    logger.info(f"Predict SQL is {issue_sql}\n\n")
    logger.info(f"Solution SQL is {sol_sql}")
    test_case_code = "from datetime import date\n" + test_code
    test_case_code += (
        "\n__test_case_result__ = test_case(pred_sqls, sol_sqls, db_name, conn)"
    )

    logger.info(f"Test case content:\n{test_case_code}")
    logger.info(f"Executing test case")

    old_stdout = sys.stdout
    mystdout = io.StringIO()
    sys.stdout = mystdout

    try:
        exec(test_case_code, global_env, local_env)
        logger.info(f"Test case passed.")
        test_passed = True
        error_message = ""
    except AssertionError as e:
        logger.error(f"Test case failed due to assertion error: {e}")
        error_message = f"Test case failed due to assertion error: {e}\n"
        test_passed = False
    except Exception as e:
        logger.error(f"Test case failed due to error: {e}")
        error_message = f"Test case failed due to error: {e}\n"
        test_passed = False
    finally:
        sys.stdout = old_stdout

    captured_output = mystdout.getvalue()
    if captured_output.strip():
        logger.info(f"Captured output from test_code:\n{captured_output}")

    return test_passed, error_message


def execute_test_cases(
    test_cases, sql_result, logger, conn, issue_sql, sol_sql, db_name
):
    """
    Execute test cases sequentially.
    Returns (passed_count, failed_tests, error_messages).
    """
    passed_count = 0
    failed_tests = []
    test_error_messages = ""

    for i, test_case in enumerate(test_cases, start=1):
        logger.info(f"Starting test case {i}/{len(test_cases)}")

        try:
            test_passed, error_message = run_test_case(
                test_case, sql_result, logger, conn, issue_sql, sol_sql, db_name
            )

            if test_passed:
                passed_count += 1
            else:
                failed_tests.append(f"test_{i}")
                test_error_messages += error_message

        except Exception as e:
            logger.error(f"Unexpected error executing test case {i}: {e}")
            failed_tests.append(f"test_{i}")
            test_error_messages += f"Unexpected error in test case {i}: {str(e)}\n"

    return passed_count, failed_tests, test_error_messages


def run_solution_phase(
    issue_sql, sol_sql, gold_sql, db_name, test_cases, logger, conn, efficiency
):
    """
    Execute sol_sql and validate its results using test cases.
    If efficiency=True, there may be additional performance comparisons.
    """
    sol_sql_result, exec_error_flag, timeout_flag = execute_queries(
        sol_sql, db_name, conn, logger, "LLM Generated SQL", is_solution=True
    )

    instance_execution_error = exec_error_flag
    instance_timeout_error = timeout_flag
    instance_assertion_error = False
    passed_count = 0
    failed_tests = []
    error_msg = ""

    if not instance_execution_error and not instance_timeout_error and test_cases:
        if not efficiency:
            passed_count, failed_tests, test_error_messages = execute_test_cases(
                test_cases, sol_sql_result, logger, conn, sol_sql, gold_sql, db_name
            )
        else:
            passed_count, failed_tests, test_error_messages = execute_test_cases(
                test_cases, sol_sql_result, logger, conn, issue_sql, sol_sql, db_name
            )
        if failed_tests:
            instance_assertion_error = True
            error_msg = test_error_messages

    return (
        instance_execution_error,
        instance_timeout_error,
        instance_assertion_error,
        passed_count,
        failed_tests,
        error_msg,
    )


def ensure_safe_connection_close(conn, db_name, logger):
    """Safely close a database connection with error handling"""
    if conn:
        try:
            logger.info(f"Closing connection to database {db_name}")
            close_sqlserver_connection(conn)
            logger.info(f"Connection to {db_name} closed successfully")
        except Exception as e:
            logger.error(f"Error closing connection to {db_name}: {e}")
            # Try a more aggressive approach if normal close fails
            try:
                logger.warning(f"Attempting forceful connection close for {db_name}")
                # Additional close logic could be added here if needed
                conn = None
                gc.collect()
            except Exception as e2:
                logger.error(f"Forceful connection close also failed: {e2}")
                pass


def ensure_safe_database_reset(db_name, logger, max_retries=3):
    """Safely reset a database with retries and error handling"""
    for attempt in range(max_retries):
        try:
            logger.info(
                f"Resetting database {db_name} (attempt {attempt+1}/{max_retries})"
            )
            reset_and_restore_database(db_name, logger)
            logger.info(f"Database {db_name} reset successfully")
            return True
        except Exception as e:
            logger.error(
                f"Error resetting database {db_name} (attempt {attempt+1}): {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(3)  # Wait before retry
            else:
                logger.error(
                    f"Failed to reset database {db_name} after {max_retries} attempts"
                )
                return False


def evaluate_instance(data, args, logger):
    """Evaluate a single instance and return the results."""
    # Initialize result values
    instance_id = data.get("instance_id", "unknown")
    error_message = ""
    solution_phase_execution_error = False
    solution_phase_timeout_error = False
    solution_phase_assertion_error = False
    passed_test_cases_count = 0
    failed_test_cases = []
    solution_error_msg = ""

    # Check for required fields
    required_fields = [
        "db_id",
        "preprocess_sql",
        "issue_sql",
        "sol_sql",
    ]

    if args.mode == "pred":
        required_fields.append("pred_sqls")

    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        logger.error(f"Missing required fields: {', '.join(missing_fields)}")
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": f"Missing fields: {', '.join(missing_fields)}",
            "total_test_cases": len(data.get("test_cases", [])),
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "solution_phase_execution_error": True,
            "solution_phase_timeout_error": False,
            "solution_phase_assertion_error": False,
        }

    # Extract data
    efficiency = data.get("efficiency", False)
    db_name = data["db_id"]
    preprocess_sql = split_field(data, "preprocess_sql")
    issue_sql = split_field(data, "issue_sql")
    clean_up_sql = split_field(data, "clean_up_sql")
    test_cases = data.get("test_cases", [])
    total_test_cases = len(test_cases)

    # Which solution field to use depends on --mode
    if args.mode == "gold":
        sol_sql = split_field(data, "sol_sql")
        gold_sql = split_field(data, "sol_sql")
    else:
        sol_sql = split_field(data, "pred_sqls")
        gold_sql = split_field(data, "sol_sql")

    # Declare connections as None to ensure they're in scope for the finally block
    error_conn = None
    solution_conn = None

    try:
        # Get connection with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                error_conn = get_connection_for_phase(db_name, logger)
                if error_conn:
                    break
            except Exception as e:
                logger.error(f"Failed to get connection on attempt {attempt+1}: {e}")
                if attempt == max_retries - 1:
                    return {
                        "instance_id": instance_id,
                        "status": "failed",
                        "error_message": f"Failed to get database connection after {max_retries} attempts",
                        "total_test_cases": total_test_cases,
                        "passed_test_cases": 0,
                        "failed_test_cases": [],
                        "solution_phase_execution_error": True,
                        "solution_phase_timeout_error": False,
                        "solution_phase_assertion_error": False,
                    }
                time.sleep(3)  # Wait before retry

        # ---------- Solution Phase ----------
        logger.info("=== Starting Solution Phase ===")

        # Get new connection for solution phase with retries
        for attempt in range(max_retries):
            try:
                solution_conn = get_connection_for_phase(db_name, logger)
                if solution_conn:
                    break
            except Exception as e:
                logger.error(
                    f"Failed to get solution phase connection on attempt {attempt+1}: {e}"
                )
                if attempt == max_retries - 1:
                    return {
                        "instance_id": instance_id,
                        "status": "failed",
                        "error_message": f"Failed to get solution phase database connection after {max_retries} attempts",
                        "total_test_cases": total_test_cases,
                        "passed_test_cases": 0,
                        "failed_test_cases": [],
                        "solution_phase_execution_error": True,
                        "solution_phase_timeout_error": False,
                        "solution_phase_assertion_error": False,
                    }
                time.sleep(3)  # Wait before retry

        # Run preprocessing SQL again
        run_preprocessing(preprocess_sql, db_name, logger, solution_conn)

        # Run solution phase tests
        (
            solution_phase_execution_error,
            solution_phase_timeout_error,
            solution_phase_assertion_error,
            passed_count,
            failed_tests,
            solution_error_msg,
        ) = run_solution_phase(
            issue_sql,
            sol_sql,
            gold_sql,
            db_name,
            test_cases,
            logger,
            solution_conn,
            efficiency,
        )

        passed_test_cases_count = passed_count
        failed_test_cases = failed_tests

        # Cleanup SQL
        if clean_up_sql:
            logger.info("Executing Clean Up SQL after solution phase.")
            try:
                execute_queries(
                    clean_up_sql,
                    db_name,
                    solution_conn,
                    logger,
                    section_title="Clean Up SQL",
                )
            except Exception as e:
                logger.error(f"Error executing cleanup SQL: {e}")

        # Close connection after solution phase
        ensure_safe_connection_close(solution_conn, db_name, logger)
        solution_conn = None
        logger.info("=== Solution Phase Completed ===")

        # Final database reset with retries and extended error handling
        logger.info(f"Performing final database reset for {db_name}")
        ensure_safe_database_reset(db_name, logger, max_retries=5)
        logger.info("Final database reset completed")

    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"Unexpected error evaluating instance: {e}")
        logger.error(traceback.format_exc())

        # Make sure to close connections and reset database
        if error_conn:
            try:
                ensure_safe_connection_close(error_conn, db_name, logger)
            except Exception as conn_e:
                logger.error(f"Error closing error_conn: {conn_e}")

        if solution_conn:
            try:
                ensure_safe_connection_close(solution_conn, db_name, logger)
            except Exception as conn_e:
                logger.error(f"Error closing solution_conn: {conn_e}")

        try:
            ensure_safe_database_reset(db_name, logger, max_retries=5)
        except Exception as reset_e:
            logger.error(f"Final emergency database reset failed: {reset_e}")

        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": f"Unexpected error: {str(e)}",
            "total_test_cases": total_test_cases,
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "solution_phase_execution_error": True,
            "solution_phase_timeout_error": False,
            "solution_phase_assertion_error": False,
        }
    finally:
        # Final cleanup in all cases
        if error_conn:
            try:
                ensure_safe_connection_close(error_conn, db_name, logger)
            except:
                pass

        if solution_conn:
            try:
                ensure_safe_connection_close(solution_conn, db_name, logger)
            except:
                pass

        # Force garbage collection
        gc.collect()

    # Determine overall status
    ret_status = "success"
    if (
        solution_phase_execution_error
        or solution_phase_timeout_error
        or solution_phase_assertion_error
    ):
        ret_status = "failed"

    # Return results with error message
    final_error_message = solution_error_msg if solution_error_msg else error_message

    return {
        "instance_id": instance_id,
        "status": ret_status,
        "error_message": final_error_message,
        "total_test_cases": total_test_cases,
        "passed_test_cases": passed_test_cases_count,
        "failed_test_cases": failed_test_cases,
        "solution_phase_execution_error": solution_phase_execution_error,
        "solution_phase_timeout_error": solution_phase_timeout_error,
        "solution_phase_assertion_error": solution_phase_assertion_error,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Execute a single SQL solution and test case (MS SQL Server)."
    )
    parser.add_argument(
        "--jsonl_file",
        help="Path to the JSONL file containing the dataset instance.",
        required=True,
    )
    parser.add_argument(
        "--output_file",
        required=True,
        help="Path to the JSONL file for output with evaluation results.",
    )
    parser.add_argument(
        "--mode", help="gold or pred", choices=["gold", "pred"], default="gold"
    )
    parser.add_argument(
        "--logging",
        type=str,
        default="false",
        help="Enable or disable logging ('true' or 'false').",
    )
    parser.add_argument(
        "--log_file",
        type=str,
        help="Specific path for the log file.",
    )

    args = parser.parse_args()

    try:
        # Load the data (expecting only one instance)
        data_list = load_jsonl(args.jsonl_file)
        if not data_list:
            print("No data found in the JSONL file.")
            sys.exit(1)

        data = data_list[0]  # Get the single instance
        instance_id = data.get("instance_id", 0)

        # Configure logger
        if args.logging == "true":
            if args.log_file:
                log_filename = args.log_file
            else:
                log_filename = (
                    os.path.splitext(args.jsonl_file)[0]
                    + f"_instance_{instance_id}.log"
                )
            logger = configure_logger(log_filename)
            print(f"Logging to {log_filename}")
        else:
            logger = NullLogger()

        logger.info(f"Evaluating instance {instance_id}")

        # Evaluate the instance
        evaluation_result = evaluate_instance(data, args, logger)

        # Write the output
        with open(args.output_file, "w") as f:
            json.dump(evaluation_result, f)

        # Exit with success code
        sys.exit(0)
    except Exception as e:
        print(f"Error evaluating instance: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
