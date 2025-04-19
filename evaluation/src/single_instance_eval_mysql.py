#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evaluation script for a single SQL instance (MySQL version)
This is designed to be called from wrapper_evaluation_mysql.py
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
from mysql_utils import (
    perform_query_on_mysql_databases,
    close_mysql_connection,
    execute_queries,
    reset_and_restore_database,
    load_jsonl,
    split_field,
)
from mysql_test_utils import (
    check_sql_function_usage,
    remove_distinct,
    preprocess_results,
    ex_base,
    performance_compare_by_qep,
)


def run_test_case(test_code, result, logger, conn, issue_sql, sol_sql, db_name):
    """
    Execute a single test case, capturing AssertionError or other exceptions.
    Returns True if test passed, False otherwise, and an error message.
    """
    global_env = {
        "perform_query_on_mysql_databases": perform_query_on_mysql_databases,
        "execute_queries": execute_queries,
        "ex_base": ex_base,
        "performance_compare_by_qep": performance_compare_by_qep,
        "check_sql_function_usage": check_sql_function_usage,
        "remove_distinct": remove_distinct,
        "preprocess_results": preprocess_results,
        "pred_query_result": result,
        "date": date,
    }
    local_env = {
        "conn": conn,
        "pred_sqls": issue_sql,
        "sol_sqls": sol_sql,
        "db_name": db_name,
    }

    logger.info(f"Executing test case")
    logger.info(
        "from datetime import date\n"
        + test_code
        + "\n__test_case_result__ = test_case(pred_sqls, sol_sqls, db_name, conn)"
    )
    old_stdout = sys.stdout
    mystdout = io.StringIO()
    sys.stdout = mystdout

    try:
        exec(
            "from datetime import date\n"
            + test_code
            + "\n__test_case_result__ = test_case(pred_sqls, sol_sqls, db_name, conn)",
            global_env,
            local_env,
        )
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


def run_preprocessing(preprocess_sql, db_name, logger, conn):
    """
    Execute any pre-processing SQL statements.
    """
    if preprocess_sql:
        execute_queries(
            preprocess_sql,
            db_name,
            conn,
            logger,
            section_title="Preprocess SQL",
            is_solution=False,
        )


def run_evaluation_phase(
    sol_sqls, gold_sqls, issue_sqls, db_name, test_cases, logger, efficiency, conn
):
    """
    1. Execute 'sol_sqls'
    2. If no errors, run test cases.
    Returns flag tuple + (passed_count, failed_tests, error_msg).
    """
    sol_sql_result, exec_error_flag, timeout_flag, error_msg = execute_queries(
        sol_sqls,
        db_name,
        conn,
        logger,
        section_title="LLM Generated SQL",
        is_solution=True,
    )

    instance_execution_error = exec_error_flag
    instance_timeout_error = timeout_flag
    instance_assertion_error = False
    passed_count = 0
    failed_tests = []

    if not instance_execution_error and not instance_timeout_error and test_cases:
        if not efficiency:
            passed_count, failed_tests, _ = execute_test_cases(
                test_cases,
                sol_sql_result,
                logger,
                conn,
                sol_sqls,
                gold_sqls,
                db_name,
            )
        else:
            passed_count, failed_tests, _ = execute_test_cases(
                test_cases,
                sol_sql_result,
                logger,
                conn,
                issue_sqls,
                sol_sqls,
                db_name,
            )

        if failed_tests:
            instance_assertion_error = True

    return (
        instance_execution_error,
        instance_timeout_error,
        instance_assertion_error,
        passed_count,
        failed_tests,
        error_msg,
    )


def get_mysql_connection(
    db_name, logger, mysql_host, mysql_port, mysql_user, mysql_pass
):
    """Get a MySQL connection for the given database"""
    try:
        _, conn = perform_query_on_mysql_databases("SELECT 1", db_name)
        return conn
    except Exception as e:
        logger.error(f"Failed to get MySQL connection for database {db_name}: {e}")
        return None


def evaluate_instance(data, args, logger):
    """Evaluate a single instance and return the results."""
    # Initialize result values
    instance_id = data.get("instance_id", "unknown")
    error_message = ""
    evaluation_phase_execution_error = False
    evaluation_phase_timeout_error = False
    evaluation_phase_assertion_error = False
    passed_test_cases_count = 0
    failed_test_cases = []
    error_msg = ""

    # Check for required fields
    required_fields = ["db_id", "issue_sql", "sol_sql"]
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
            "issue_sql_error": 1,
            "evaluation_phase_execution_error": True,
            "evaluation_phase_timeout_error": False,
            "evaluation_phase_assertion_error": False,
        }

    # Extract data
    efficiency = data.get("efficiency", False)
    db_name = data["db_id"]
    preprocess_sql = split_field(data, "preprocess_sql")
    issue_sqls = split_field(data, "issue_sql")
    clean_up_sql = split_field(data, "clean_up_sql")
    test_cases = data.get("test_cases", [])
    total_test_cases = len(test_cases)

    # MySQL connection parameters
    mysql_host = "bird_critic_mysql"
    mysql_port = 3306
    mysql_user = "root"
    mysql_pass = args.mysql_password

    # Which solution field to use depends on --mode
    if args.mode == "gold":
        sol_sqls = split_field(data, "sol_sql")
        gold_sqls = split_field(data, "sol_sql")
    else:
        sol_sqls = split_field(data, "pred_sqls")
        gold_sqls = split_field(data, "sol_sql")

    # Get connection with retry
    db_connection = None
    max_retries = 3
    for attempt in range(max_retries):
        try:
            db_connection = get_mysql_connection(
                db_name, logger, mysql_host, mysql_port, mysql_user, mysql_pass
            )
            if db_connection:
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
                    "evaluation_phase_execution_error": True,
                    "evaluation_phase_timeout_error": False,
                    "evaluation_phase_assertion_error": False,
                }
            time.sleep(3)  # Wait before retry

    try:
        _, db_connection = perform_query_on_mysql_databases("SELECT 1", db_name)

        # ---------- Evaluation Phase ----------
        logger.info("=== Starting Evaluation Phase ===")

        # Run preprocessing SQL again
        run_preprocessing(preprocess_sql, db_name, logger, db_connection)

        # Run evaluation phase tests
        (
            evaluation_phase_execution_error,
            evaluation_phase_timeout_error,
            evaluation_phase_assertion_error,
            passed_count,
            failed_tests,
            error_msg,
        ) = run_evaluation_phase(
            sol_sqls,
            gold_sqls,
            issue_sqls,
            db_name,
            test_cases,
            logger,
            efficiency,
            db_connection,
        )

        passed_test_cases_count = passed_count
        failed_test_cases = failed_tests

        # Cleanup SQL
        if clean_up_sql:
            logger.info("Executing Clean Up SQL after solution phase.")
            execute_queries(
                clean_up_sql,
                db_name,
                db_connection,
                logger,
                section_title="Clean Up SQL",
                is_solution=False,
            )

        logger.info("=== Evaluation Phase Completed ===")

    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"Unexpected error evaluating instance: {e}")
        logger.error(traceback.format_exc())
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": f"Unexpected error: {str(e)}",
            "total_test_cases": total_test_cases,
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "evaluation_phase_execution_error": True,
            "evaluation_phase_timeout_error": False,
            "evaluation_phase_assertion_error": False,
        }
    finally:
        # Close connection
        if db_connection:
            try:
                close_mysql_connection(db_name, db_connection)
                db_connection = None
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

        # Reset database one last time
        try:
            reset_and_restore_database(
                db_name,
                f"{db_name}_template",
                mysql_user,
                mysql_pass,
                mysql_host,
                mysql_port,
                logger,
            )
        except Exception as e:
            logger.error(f"Error during final database reset: {e}")

        # Force garbage collection
        gc.collect()

    # Determine overall status
    ret_status = "success"
    if (
        evaluation_phase_execution_error
        or evaluation_phase_timeout_error
        or evaluation_phase_assertion_error
    ):
        ret_status = "failed"

    # Return results
    return {
        "instance_id": instance_id,
        "status": ret_status,
        "error_message": error_msg if error_msg else error_message,
        "total_test_cases": total_test_cases,
        "passed_test_cases": passed_test_cases_count,
        "failed_test_cases": failed_test_cases,
        "evaluation_phase_execution_error": evaluation_phase_execution_error,
        "evaluation_phase_timeout_error": evaluation_phase_timeout_error,
        "evaluation_phase_assertion_error": evaluation_phase_assertion_error,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Execute a single SQL solution and test case (MySQL)."
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
    parser.add_argument(
        "--mysql_password",
        default="123123",
        help="MySQL root password for resetting the database.",
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
