#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evaluation script for a single Oracle SQL instance
This is designed to be called from wrapper_evaluation_oracle.py
"""

import argparse
import json
import sys
import os
import io
import traceback
import gc
from datetime import date

# Local imports
from logger import configure_logger, NullLogger
from oracle_utils import (
    reset_and_restore_database,
    get_connection_for_phase,
    execute_queries,
    execute_issue_sql,
    run_preprocessing,
    close_oracle_connections,
    perform_query_on_oracle_databases,
)
from oracle_test_utils import (
    ex_base,
    ex_base_dict,
    load_jsonl,
    split_field,
    performance_compare_by_execution_plan,
)
from oracle_test_utils import (
    check_sql_function_usage,
    remove_distinct,
    preprocess_results,
    preprocess_results_dict,
)


def run_test_case(test_code, result, logger, idx, conn, issue_sql, sol_sql, db_name):
    """
    Execute a single test case, capturing AssertionError or other exceptions,
    and record the result.
    """

    global_env = {
        "perform_query_on_oracle_databases": perform_query_on_oracle_databases,
        "execute_queries": execute_queries,
        "ex_base": ex_base,
        "ex_base_dict": ex_base_dict,
        "performance_compare_by_execution_plan": performance_compare_by_execution_plan,
        "check_sql_function_usage": check_sql_function_usage,
        "remove_distinct": remove_distinct,
        "preprocess_results": preprocess_results,
        "preprocess_results_dict": preprocess_results_dict,
        "pred_query_result": result,
        "date": date,
        "conn": conn,
    }
    local_env = {
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
    logger.info(f"Executing test case {idx}")

    old_stdout = sys.stdout
    mystdout = io.StringIO()
    sys.stdout = mystdout

    try:
        exec(test_case_code, global_env, local_env)
        logger.info(f"Test case {idx} passed.")
        test_passed = True
    except AssertionError as e:
        logger.error(f"Test case {idx} failed due to assertion error: {e}")
        test_passed = False
    except Exception as e:
        logger.error(f"Test case {idx} failed due to error: {e}")
        test_passed = False
    finally:
        sys.stdout = old_stdout

    captured_output = mystdout.getvalue()
    if captured_output.strip():
        logger.info(f"Captured output from test_code:\n{captured_output}")

    return test_passed


def execute_test_cases(
    test_cases, sql_result, logger, conn, issue_sql, sol_sql, db_name
):
    """
    Execute the list of test cases in sequence.
    """
    passed_count = 0
    failed_tests = []

    for i, test_case in enumerate(test_cases, start=1):
        logger.info(f"Starting test case {i}/{len(test_cases)}")
        test_passed = run_test_case(
            test_case, sql_result, logger, i, conn, issue_sql, sol_sql, db_name
        )

        if test_passed:
            passed_count += 1
        else:
            failed_tests.append(f"test_{i}")

    return passed_count, failed_tests


def run_error_phase(issue_sql, sol_sql, db_name, test_cases, logger, conn, efficiency):
    """
    1. Execute issue_sql (which is expected to fail).
    2. If it does not fail and there are test_cases, execute them and expect them to fail.
    """
    error_message, issue_sql_result = execute_issue_sql(
        issue_sql, db_name, logger, conn
    )
    assertion_error = False

    if not error_message and test_cases and not efficiency:
        passed_count, failed_tests = execute_test_cases(
            test_cases, issue_sql_result, logger, conn, issue_sql, sol_sql, db_name
        )
        if failed_tests:
            assertion_error = False
        else:
            assertion_error = True
    return error_message, issue_sql_result, assertion_error


def run_solution_phase(
    issue_sql, sol_sql, gold_sql, db_name, test_cases, logger, conn, efficiency
):
    """
    Execute sol_sql and validate its results using test cases.
    If efficiency=True, there may be additional performance comparisons.
    """
    sol_sql_result, exec_error_flag, timeout_flag, error_msg = execute_queries(
        sol_sql, db_name, conn, logger, "LLM Generated SQL", is_solution=True
    )

    instance_execution_error = exec_error_flag
    instance_timeout_error = timeout_flag
    instance_assertion_error = False
    passed_count = 0
    failed_tests = []

    if not instance_execution_error and not instance_timeout_error and test_cases:
        if not efficiency:
            passed_count, failed_tests = execute_test_cases(
                test_cases, sol_sql_result, logger, conn, sol_sql, gold_sql, db_name
            )
        else:
            passed_count, failed_tests = execute_test_cases(
                test_cases, sol_sql_result, logger, conn, issue_sql, sol_sql, db_name
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


def clean_sql_for_oracle(sql_statements):
    """
    Removes trailing semicolons and forward slashes from SQL statements
    to make them compatible with python-oracledb.

    Args:
        sql_statements: A string or list of strings containing SQL statements

    Returns:
        Cleaned SQL statement(s) in the same format as input
    """
    if isinstance(sql_statements, list):
        cleaned_statements = []
        for stmt in sql_statements:
            # Remove all trailing semicolons and forward slashes
            cleaned = stmt.strip()
            while cleaned.endswith(";") or cleaned.endswith("/"):
                cleaned = cleaned.rstrip(";/")
                cleaned = cleaned.strip()  # Remove any new trailing whitespace
            cleaned_statements.append(cleaned)
        return cleaned_statements
    else:
        # Handle single string case
        cleaned = sql_statements.strip()
        while cleaned.endswith(";") or cleaned.endswith("/"):
            cleaned = cleaned.rstrip(";/")
            cleaned = cleaned.strip()  # Remove any new trailing whitespace
        return cleaned


def evaluate_instance(data, ephemeral_user, args, logger):
    """Evaluate a single instance and return the results."""
    # Initialize result values
    instance_id = data.get("instance_id", "unknown")
    solution_phase_execution_error = False
    solution_phase_timeout_error = False
    solution_phase_assertion_error = False
    passed_test_cases_count = 0
    failed_test_cases = []
    error_message_text = ""

    # Check for required fields
    required_fields = ["db_id", "preprocess_sql", "issue_sql", "sol_sql"]
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
        # Post-process SQL statements to remove trailing semicolons and forward slashes
        # sol_sql = clean_sql_for_oracle(sol_sql)

    # Use the provided ephemeral user
    if not ephemeral_user:
        logger.error(f"No ephemeral user provided for instance {instance_id}")
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": "No ephemeral user provided",
            "total_test_cases": total_test_cases,
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "solution_phase_execution_error": True,
            "solution_phase_timeout_error": False,
            "solution_phase_assertion_error": False,
        }

    # Attempt to process the instance
    try:

        # ---------- Solution Phase ----------
        logger.info("=== Starting Solution Phase ===")
        solution_conn = get_connection_for_phase(ephemeral_user, logger)
        if not solution_conn:
            logger.error(
                f"Failed to connect to Oracle database for solution phase: {ephemeral_user}"
            )
            return {
                "instance_id": instance_id,
                "status": "failed",
                "error_message": "Failed to connect to Oracle database for solution phase",
                "total_test_cases": total_test_cases,
                "passed_test_cases": 0,
                "failed_test_cases": [],
                "solution_phase_execution_error": True,
                "solution_phase_timeout_error": False,
                "solution_phase_assertion_error": False,
            }

        # Run preprocessing SQL again
        try:
            run_preprocessing(preprocess_sql, ephemeral_user, logger, solution_conn)
        except Exception as e:
            logger.error(f"Error during preprocessing in solution phase: {e}")
            close_oracle_connections(solution_conn)
            return {
                "instance_id": instance_id,
                "status": "failed",
                "error_message": f"Error during preprocessing in solution phase: {str(e)}",
                "total_test_cases": total_test_cases,
                "passed_test_cases": 0,
                "failed_test_cases": [],
                "solution_phase_execution_error": True,
                "solution_phase_timeout_error": False,
                "solution_phase_assertion_error": False,
            }

        # Run solution phase tests
        (
            solution_phase_execution_error,
            solution_phase_timeout_error,
            solution_phase_assertion_error,
            passed_count,
            failed_tests,
            error_msg,
        ) = run_solution_phase(
            issue_sql,
            sol_sql,
            gold_sql,
            ephemeral_user,
            test_cases,
            logger,
            solution_conn,
            efficiency,
        )

        passed_test_cases_count = passed_count
        failed_test_cases = failed_tests
        error_message_text += error_msg

        # Cleanup SQL
        if clean_up_sql:
            logger.info("Executing Clean Up SQL after solution phase.")
            try:
                execute_queries(
                    clean_up_sql,
                    ephemeral_user,
                    solution_conn,
                    logger,
                    section_title="Clean Up SQL",
                )
            except Exception as e:
                logger.error(f"Error during cleanup SQL: {e}")
                # Continue even if cleanup fails

        # Close connection after solution phase
        close_oracle_connections(solution_conn)

        logger.info("=== Solution Phase Completed ===")

        # Reset database after solution phase
        logger.info(f"Resetting ephemeral user {ephemeral_user} after solution phase.")
        reset_and_restore_database(ephemeral_user, logger)

    except Exception as e:
        logger.error(f"Unexpected error evaluating instance: {e}")
        logger.error(traceback.format_exc())
        try:
            # Try to reset the database in case of error
            reset_and_restore_database(ephemeral_user, logger)
        except Exception as reset_error:
            logger.error(f"Error resetting database after failure: {reset_error}")

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

    # Return results
    return {
        "instance_id": instance_id,
        "status": ret_status,
        "error_message": error_message_text if error_message_text else None,
        "total_test_cases": total_test_cases,
        "passed_test_cases": passed_test_cases_count,
        "failed_test_cases": failed_test_cases,
        "solution_phase_execution_error": solution_phase_execution_error,
        "solution_phase_timeout_error": solution_phase_timeout_error,
        "solution_phase_assertion_error": solution_phase_assertion_error,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Execute a single SQL solution and test case (Oracle)."
    )
    parser.add_argument(
        "--jsonl_file",
        help="Path to the JSONL file containing the dataset instance.",
        required=True,
    )
    parser.add_argument(
        "--output_file",
        required=True,
        help="Path to the JSON file for output with evaluation results.",
    )
    parser.add_argument(
        "--mode", help="gold or pred", choices=["gold", "pred"], default="pred"
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
        "--ephemeral_user",
        required=True,
        help="The ephemeral Oracle user to use for this evaluation.",
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

        logger.info(
            f"Evaluating instance {instance_id} with ephemeral user {args.ephemeral_user}"
        )

        # Evaluate the instance
        evaluation_result = evaluate_instance(data, args.ephemeral_user, args, logger)

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
