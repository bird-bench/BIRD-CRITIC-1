#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evaluation Script for SQL Debugging Test Cases (PostgreSQL Edition)

This script:
1. Resets and restores a PostgreSQL database to a known initial state.
2. Runs preprocessing SQL queries to set up the environment.
3. Runs error queries expecting them to fail.
4. If error queries do not fail, runs test cases that should fail.
   Closes the DB connection at the end of the error scenario phase.
5. Resets the database again.
6. Runs solution queries to get the intended results.
7. Runs test cases to validate the solution.
8. Runs clean-up queries.
9. Closes the DB connection and produces a final report.
"""

import argparse
import json
import logging
import sys
import os
import subprocess
from datetime import datetime
import re
import threading
import io
import tqdm

import psycopg2
from psycopg2 import OperationalError
import multiprocessing
from test_utils import check_sql_function_usage, remove_distinct, preprocess_results

sys.path.append("/app/SO_evaluation")

from stack_overflow_scripts.postgresql_setup import (
    perform_query_on_postgresql_databases,
    close_postgresql_connection,
    close_postgresql_pool,
    close_all_postgresql_pools,
    POST_DATABASE_MAPPING,
    TABLE_ORDER,
)

# Global counters
number_of_execution_errors = 0
number_of_timeouts = 0
number_of_assertion_errors = 0
number_of_error_sql_errors = 0
total_passed_instances = 0
number_error_unexpected_pass = 0
question_test_case_results = []


def load_jsonl(file_path):
    try:
        with open(file_path, "r") as file:
            return [json.loads(line) for line in file]
    except Exception as e:
        print(f"Failed to load JSONL file: {e}")
        sys.exit(1)


def configure_logger(log_filename):
    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def log_section_header(section_title, logger):
    separator = f"{'=' * 20} {section_title} {'=' * 20}"
    logger.info(f"\n\n{separator}\n")


def log_section_footer(logger):
    separator = f"{'=' * 60}"
    logger.info(f"\n\n{separator}\n")


def reset_and_restore_database(db_name, pg_password, logger=None):
    """
    Resets the specified database by creating it from the corresponding template database.
    Steps:
    1) Close the connection pool
    2) Terminate all connections
    3) dropdb
    4) createdb --template
    """
    if logger is None:
        logger = PrintLogger()
    try:
        pg_host = "bird_critic_postgresql"
        pg_port = 5432
        pg_user = "root"

        env_vars = os.environ.copy()
        env_vars["PGPASSWORD"] = pg_password

        template_db_name = f"{db_name}_template"

        logger.info(f"Resetting database {db_name} using template {template_db_name}")
        logger.info(f"Closing connection pool for database {db_name} before resetting.")
        close_postgresql_pool(db_name)

        terminate_command = [
            "psql",
            "-h",
            pg_host,
            "-p",
            str(pg_port),
            "-U",
            pg_user,
            "-d",
            "postgres",
            "-c",
            f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{db_name}' AND pid <> pg_backend_pid();
            """,
        ]
        subprocess.run(
            terminate_command,
            check=True,
            env=env_vars,
            timeout=60,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(f"All connections to database {db_name} have been terminated.")

        drop_command = [
            "dropdb",
            "--if-exists",
            "-h",
            pg_host,
            "-p",
            str(pg_port),
            "-U",
            pg_user,
            db_name,
        ]
        subprocess.run(
            drop_command,
            check=True,
            env=env_vars,
            timeout=60,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(f"Database {db_name} dropped if it existed.")

        create_command = [
            "createdb",
            "-h",
            pg_host,
            "-p",
            str(pg_port),
            "-U",
            pg_user,
            db_name,
            "--template",
            template_db_name,
        ]
        subprocess.run(
            create_command,
            check=True,
            env=env_vars,
            timeout=60,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(
            f"Database {db_name} created from template {template_db_name} successfully."
        )

    except subprocess.TimeoutExpired as e:
        logger.error(f"Timeout expired while resetting {db_name} from template: {e}")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error resetting {db_name} from template: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error resetting {db_name} from template: {e}")
        sys.exit(1)


def split_field(data, field_name):
    field_value = data.get(field_name, "")
    if not field_value:
        return []
    if isinstance(field_value, str):
        # Use [split] as the delimiter
        sql_statements = [
            stmt.strip()
            for stmt in re.split(r"\[split\]\s*", field_value)
            if stmt.strip()
        ]
        return sql_statements
    elif isinstance(field_value, list):
        return field_value
    else:
        return []


def get_connection_for_phase(db_name, logger):
    """
    Acquires a dedicated connection for the specified phase.
    """
    logger.info(f"Acquiring dedicated connection for phase on db: {db_name}")
    _, conn = perform_query_on_postgresql_databases("SELECT 1", db_name, conn=None)
    return conn


class NullLogger:
    """A logger that does not output any message."""

    def info(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


class PrintLogger:
    """A Logger implementation that prints messages to stdout."""

    def info(self, msg, *args, **kwargs):
        print(f"[INFO] {msg}", *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        print(f"[ERROR] {msg}", *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        print(f"[WARNING] {msg}", *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        print(f"[DEBUG] {msg}", *args, **kwargs)


def execute_queries(
    queries, db_name, conn=None, logger=None, section_title="", is_solution=True
):
    """
    Executes a list of queries using the same connection (conn).
    """
    if logger is None:
        logger = PrintLogger()

    log_section_header(section_title, logger)
    query_result = None
    execution_error = False
    timeout_error = False

    for i, query in enumerate(queries):
        try:
            logger.info(f"Executing query {i+1}/{len(queries)}: {query}")
            query_result, conn = perform_query_on_postgresql_databases(
                query, db_name, conn=conn
            )
            logger.info(f"Query result: {query_result}")
        except psycopg2.errors.QueryCanceled as e:
            logger.error(f"Timeout error executing query {i}: {e}")
            if is_solution:
                timeout_error = True
        except OperationalError as e:
            logger.error(f"OperationalError executing query {i}: {e}")
            if is_solution:
                execution_error = True
        except psycopg2.Error as e:
            logger.error(f"psycopg2 Error executing query {i}: {e}")
            if is_solution:
                execution_error = True
        except subprocess.TimeoutExpired as e:
            logger.error(f"Subprocess timeout executing query {i}: {e}")
            if is_solution:
                timeout_error = True
        except Exception as e:
            logger.error(f"Generic error executing query {i}: {e}")
            if is_solution:
                execution_error = True
        finally:
            logger.info(f"[{section_title}] DB: {db_name}, conn info: {conn}")

    log_section_footer(logger)
    return query_result, execution_error, timeout_error


def execute_error_sql(error_sql_list, db_name, logger, conn):
    """
    Executes a list of SQL statements that are expected to produce errors
    using the same connection. Returns the first encountered error_message,
    and the result if no error happened.
    """
    log_section_header("Error Reproduction", logger)
    error_message = None
    error_sql_result = None

    if not error_sql_list:
        logger.warning("No error SQL provided for reproduction.")
    else:
        for i, query in enumerate(error_sql_list):
            try:
                logger.info(
                    f"Executing error query {i+1}/{len(error_sql_list)}: {query}"
                )
                query_result, conn = perform_query_on_postgresql_databases(
                    query, db_name, conn
                )
            except psycopg2.errors.QueryCanceled as e:
                logger.error(f"Timeout error executing error SQL {i}: {e}")
                error_message = str(e)
                break
            except psycopg2.Error as e:
                logger.info(f"Expected error encountered for SQL {i}: {e}")
                error_message = str(e)
                break
            except subprocess.TimeoutExpired as e:
                logger.error(f"Subprocess timeout executing error SQL {i}: {e}")
                error_message = f"Timeout: {str(e)}"
                break
            except Exception as e:
                logger.info(f"Expected error encountered for SQL {i}: {e}")
                error_message = str(e)
                break
            finally:
                logger.info(f"[Error SQL] DB: {db_name}, conn: {conn}")
    log_section_footer(logger)
    return error_message, error_sql_result


def run_test_case(
    test_code, result, logger, idx, return_dict, conn, error_sql, sol_sql, db_name
):
    global_env = {
        "perform_query_on_postgresql_databases": perform_query_on_postgresql_databases,
        "execute_queries": execute_queries,
        "ex_base": ex_base,
        "performance_compare_by_qep": performance_compare_by_qep,
        "check_sql_function_usage": check_sql_function_usage,
        "remove_distinct": remove_distinct,
        "preprocess_results": preprocess_results,
        "pred_query_result": result,
    }
    local_env = {
        "conn": conn,
        "pred_sqls": error_sql,
        "sol_sqls": sol_sql,
        "db_name": db_name,
    }

    logger.info(f"Passing result is {result}")

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
        return_dict[idx] = "passed"
    except AssertionError as e:
        logger.error(f"Test case {idx} failed due to assertion error: {e}")
        return_dict[idx] = "failed"
    except Exception as e:
        logger.error(f"Test case {idx} failed due to error: {e}")
        return_dict[idx] = "failed"
    finally:
        sys.stdout = old_stdout

    captured_output = mystdout.getvalue()
    if captured_output.strip():
        logger.info(f"Captured output from test_code:\n{captured_output}")


def execute_test_cases(
    test_cases, sql_result, logger, conn, error_sql, sol_sql, db_name
):
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    processes = []
    for i, test_case in enumerate(test_cases, start=1):
        logger.info(f"Starting test case {i}/{len(test_cases)}")
        p = multiprocessing.Process(
            target=run_test_case,
            args=(
                test_case,
                sql_result,
                logger,
                i,
                return_dict,
                conn,
                error_sql,
                sol_sql,
                db_name,
            ),
        )
        p.start()
        p.join(timeout=60)
        if p.is_alive():
            logger.error(f"Test case {i} execution timed out.")
            p.terminate()
            p.join()
            return_dict[i] = "timeout"
        processes.append(p)

    passed_count = 0
    failed_tests = []
    for idx in range(1, len(test_cases) + 1):
        status = return_dict.get(idx, "failed")
        if status == "passed":
            passed_count += 1
        else:
            failed_tests.append(f"test_{idx}")
    return passed_count, failed_tests


def run_preprocessing(preprocess_sql, db_name, logger, conn):
    """
    Executes preprocessing queries if any.
    """
    if preprocess_sql:
        execute_queries(preprocess_sql, db_name, conn, logger, "Preprocess SQL", False)


def run_error_phase(error_sql, sol_sql, db_name, test_cases, logger, conn, efficiency):
    """
    1. Execute error queries (expected to fail).
    2. If no error occurs, run test cases that are expected to fail.
    """
    error_message, error_sql_result = execute_error_sql(
        error_sql, db_name, logger, conn
    )
    assertion_error = False

    if error_message is None and test_cases and not efficiency:
        passed_count, failed_tests = execute_test_cases(
            test_cases, error_sql_result, logger, conn, error_sql, sol_sql, db_name
        )
        if failed_tests:
            assertion_error = False
        else:
            assertion_error = True
    return error_message, error_sql_result, assertion_error


def run_solution_phase(
    error_sql, sol_sql, db_name, test_cases, logger, conn, efficiency
):
    """
    Executes the solution queries and runs test cases on the results.
    """
    sol_sql_result, exec_error_flag, timeout_flag = execute_queries(
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
                test_cases, sol_sql_result, logger, conn, sol_sql, sol_sql, db_name
            )
        else:
            passed_count, failed_tests = execute_test_cases(
                test_cases, sol_sql_result, logger, conn, error_sql, sol_sql, db_name
            )
        if failed_tests:
            instance_assertion_error = True

    return (
        instance_execution_error,
        instance_timeout_error,
        instance_assertion_error,
        passed_count,
        failed_tests,
    )


def ex_base(pred_sqls, sol_sqls, db_name, conn):
    """
    Compares the results of pred_sqls and sol_sqls on the same database connection.
    Returns 1 if both sets of queries produce identical sets of rows, otherwise 0.
    """
    if not pred_sqls or not sol_sqls:
        return 0

    def calculate_ex(predicted_res, ground_truth_res):
        return 1 if set(predicted_res) == set(ground_truth_res) else 0

    predicted_res, pred_execution_error, pred_timeout_error = execute_queries(
        pred_sqls, db_name, conn, None, "", True
    )
    ground_truth_res, gt_execution_error, gt_timeout_error = execute_queries(
        sol_sqls, db_name, conn, None, "", True
    )

    if (
        gt_execution_error
        or gt_timeout_error
        or pred_execution_error
        or pred_timeout_error
    ):
        return 0

    if not predicted_res or not ground_truth_res:
        return 0

    predicted_res = preprocess_results(predicted_res)
    ground_truth_res = preprocess_results(ground_truth_res)
    return calculate_ex(predicted_res, ground_truth_res)


def performance_compare_by_qep(error_sqls, sol_sqls, db_name, conn):
    """
    Compare total plan cost of error_sqls vs. sol_sqls in one connection,
    by using transactions + ROLLBACK to ensure each group sees the same initial state.

    Returns 1 if sol_sqls total plan cost is lower, otherwise 0.

    Notes:
      - If error_sqls / sol_sqls contain schema changes or data modifications,
        we rely on transaction rollback to discard those changes before measuring the other side.
      - EXPLAIN itself does not execute the query, only returns the plan and cost estimate.
      - This approach will not reflect persistent changes made by error_sqls if they are needed by sol_sqls.
        Instead, it ensures both sets see the same starting state for cost comparison.
    """

    if not error_sqls or not sol_sqls:
        print("Either error_sqls or sol_sqls is empty. Returning 0.")
        return 0
    print(f"Old SQLs are {error_sqls}")
    print(f"New SQLs are {sol_sqls}")

    def measure_sqls_cost(sql_list):
        """
        Measure the sum of 'Total Cost' for each DML statement in sql_list
        via EXPLAIN (FORMAT JSON). Non-DML statements are just executed (if needed),
        but not included in the total cost.
        """
        total_cost = 0.0
        for sql in sql_list:
            upper_sql = sql.strip().upper()
            # We only measure DML cost for SELECT/INSERT/UPDATE/DELETE
            if not (
                upper_sql.startswith("SELECT")
                or upper_sql.startswith("INSERT")
                or upper_sql.startswith("UPDATE")
                or upper_sql.startswith("WITH")
                or upper_sql.startswith("DELETE")
            ):
                print(f"[measure_sqls_cost] Skip EXPLAIN for non-DML: {sql}")
                try:
                    perform_query_on_postgresql_databases(sql, db_name, conn=conn)
                except Exception as exc:
                    print(f"[measure_sqls_cost] Error executing non-DML '{sql}': {exc}")
                continue

            explain_sql = f"EXPLAIN (FORMAT JSON) {sql}"
            try:
                result_rows, _ = perform_query_on_postgresql_databases(
                    explain_sql, db_name, conn=conn
                )
                if not result_rows:
                    print(f"[measure_sqls_cost] No result returned for EXPLAIN: {sql}")
                    continue

                explain_json = result_rows[0][0]
                if isinstance(explain_json, str):
                    explain_json = json.loads(explain_json)

                if isinstance(explain_json, list) and len(explain_json) > 0:
                    plan_info = explain_json[0].get("Plan", {})
                    total_cost_part = plan_info.get("Total Cost", 0.0)
                else:
                    print(
                        f"[measure_sqls_cost] Unexpected EXPLAIN JSON format for {sql}, skip cost."
                    )
                    total_cost_part = 0.0

                total_cost += float(total_cost_part)

            except psycopg2.Error as e:
                print(f"[measure_sqls_cost] psycopg2 Error on SQL '{sql}': {e}")
            except Exception as e:
                print(f"[measure_sqls_cost] Unexpected error on SQL '{sql}': {e}")

        return total_cost

    # --- Measure cost for error_sqls ---
    try:
        # Start a transaction
        perform_query_on_postgresql_databases("BEGIN", db_name, conn=conn)
        old_total_cost = measure_sqls_cost(error_sqls)
        print(f"Old SQLs total plan cost: {old_total_cost}")
    finally:
        # Always rollback so that error_sqls changes are not visible to sol_sqls
        perform_query_on_postgresql_databases("ROLLBACK", db_name, conn=conn)

    # --- Measure cost for sol_sqls ---
    try:
        # New transaction
        perform_query_on_postgresql_databases("BEGIN", db_name, conn=conn)
        sol_total_cost = measure_sqls_cost(sol_sqls)
        print(f"Solution SQLs total plan cost: {sol_total_cost}")
    finally:
        # Rollback so that sol_sqls changes are not persisted
        perform_query_on_postgresql_databases("ROLLBACK", db_name, conn=conn)

    # Compare final costs
    print(
        f"[performance_compare_by_qep] Compare old({old_total_cost}) vs. sol({sol_total_cost})"
    )
    return 1 if sol_total_cost < old_total_cost else 0


def main():
    global number_of_execution_errors, number_of_timeouts, number_of_assertion_errors
    global total_passed_instances, number_of_error_sql_errors, number_error_unexpected_pass
    global question_test_case_results

    parser = argparse.ArgumentParser(
        description="Execute SQL solution and test cases (PostgreSQL)."
    )
    parser.add_argument(
        "--jsonl_file",
        help="Path to the JSONL file containing the dataset instance.",
        required=True,
    )
    parser.add_argument(
        "--pg_password",
        help="PostgreSQL password for resetting the database.",
        default="123123",
    )
    parser.add_argument(
        "--mode", help="gold or pred", choices=["gold", "pred"], default="pred"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of instances to process.",
        default=None,
    )
    args = parser.parse_args()

    data_list = load_jsonl(args.jsonl_file)
    if not data_list:
        print("No data found in the JSONL file.")
        sys.exit(1)

    if args.limit is not None:
        data_list = data_list[: args.limit]

    error_messages = []
    for i, data in tqdm.tqdm(enumerate(data_list), desc="Evaluating questions..."):
        instance_id = data_list[i]["instance_id"]
        log_filename = (
            os.path.splitext(args.jsonl_file)[0] + f"_instance_{instance_id}.log"
        )
        logger = configure_logger(log_filename)
        logger.info(f"Starting execution for question {instance_id}")

        required_fields = [
            "selected_database",
            "preprocess_sql",
            "error_sql",
            "sol_sql",
        ]
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            logger.error(f"Missing required fields: {', '.join(missing_fields)}")
            number_of_error_sql_errors += 1
            error_messages.append(f"Missing fields: {', '.join(missing_fields)}")
            question_test_case_results.append(
                {
                    "instance_id": instance_id,
                    "total_test_cases": len(data.get("test_cases", [])),
                    "passed_test_cases": 0,
                    "failed_test_cases": [],
                    "error_sql_error": 1,
                    "error_phase_unexpected_pass": 0,
                }
            )
            continue

        efficiency = data.get("efficiency", False)
        db_name = data["selected_database"]
        preprocess_sql = split_field(data, "preprocess_sql")
        error_sql = split_field(data, "error_sql")

        if args.mode == "gold":
            sol_sql = split_field(data, "sol_sql")
        else:
            sol_sql = split_field(data, "pred_sqls")

        clean_up_sql = split_field(data, "clean_up_sql")
        test_cases = data.get("test_cases", [])
        language = data.get("language", "postgresql")

        error_phase_unexpected_pass = 0
        solution_phase_execution_error = False
        solution_phase_timeout_error = False
        solution_phase_assertion_error = False

        total_test_cases = len(test_cases)
        passed_test_cases_count = 0
        failed_test_cases = []

        try:
            logger.info("=== Starting Error Phase ===")
            error_conn = get_connection_for_phase(db_name, logger)
            run_preprocessing(preprocess_sql, db_name, logger, error_conn)

            error_message, error_sql_result, error_assertion = run_error_phase(
                error_sql, sol_sql, db_name, test_cases, logger, error_conn, efficiency
            )
            error_messages.append(error_message if error_message else "")

            close_postgresql_connection(db_name, error_conn)

            if error_message is None and error_assertion:
                logger.info(
                    "Error SQL did not raise an error, and test cases unexpectedly passed."
                )
                error_phase_unexpected_pass = 1
            elif error_message is None and not error_assertion:
                logger.info(
                    "Error SQL did not raise an error, but test cases failed as expected."
                )

            logger.info("=== Error Phase Completed ===")

            logger.info("=== Starting Solution Phase ===")
            logger.info(f"Resetting database {db_name} for solution phase.")
            reset_and_restore_database(db_name, args.pg_password, logger)

            solution_conn = get_connection_for_phase(db_name, logger)
            run_preprocessing(preprocess_sql, db_name, logger, solution_conn)

            (
                sol_exec_err,
                sol_timeout_err,
                sol_assert_err,
                passed_count,
                failed_tests_phase2,
            ) = run_solution_phase(
                error_sql,
                sol_sql,
                db_name,
                test_cases,
                logger,
                solution_conn,
                efficiency,
            )

            close_postgresql_connection(db_name, solution_conn)

            solution_phase_execution_error = sol_exec_err
            solution_phase_timeout_error = sol_timeout_err
            solution_phase_assertion_error = sol_assert_err

            passed_test_cases_count += passed_count
            failed_test_cases.extend(failed_tests_phase2)

            if clean_up_sql:
                logger.info("Executing Clean Up SQL after solution phase.")
                new_temp_conn = get_connection_for_phase(db_name, logger)
                execute_queries(
                    clean_up_sql,
                    db_name,
                    new_temp_conn,
                    logger,
                    section_title="Clean Up SQL",
                )
                close_postgresql_connection(db_name, new_temp_conn)

            logger.info("=== Solution Phase Completed ===")
            logger.info(f"Resetting database {db_name} and restoring tables.")
            reset_and_restore_database(db_name, args.pg_password, logger)
            logger.info("Database reset and tables restored.")
        except Exception as e:
            logger.error(f"Error during execution for question {instance_id}: {e}")
            solution_phase_execution_error = True
            error_messages.append(str(e))

        if error_phase_unexpected_pass:
            number_of_error_sql_errors += 1
            number_error_unexpected_pass += 1
        if solution_phase_execution_error:
            number_of_execution_errors += 1
        if solution_phase_timeout_error:
            number_of_timeouts += 1
        if solution_phase_assertion_error:
            number_of_assertion_errors += 1

        if (
            not solution_phase_execution_error
            and not solution_phase_timeout_error
            and not solution_phase_assertion_error
        ):
            total_passed_instances += 1

        question_test_case_results.append(
            {
                "instance_id": instance_id,
                "total_test_cases": total_test_cases,
                "passed_test_cases": passed_test_cases_count,
                "failed_test_cases": failed_test_cases,
                "error_sql_error": 1 if error_phase_unexpected_pass else 0,
                "error_phase_unexpected_pass": error_phase_unexpected_pass,
                "solution_phase_execution_error": solution_phase_execution_error,
                "solution_phase_timeout_error": solution_phase_timeout_error,
                "solution_phase_assertion_error": solution_phase_assertion_error,
            }
        )

    total_instances = len(data_list)
    total_errors = (
        number_of_execution_errors
        + number_of_timeouts
        + number_of_assertion_errors
        + number_of_error_sql_errors
    )
    total_passed_instances_wo_error_pass = (
        total_passed_instances - number_error_unexpected_pass
    )
    overall_accuracy = (
        (total_passed_instances_wo_error_pass / total_instances * 100)
        if total_instances > 0
        else 0.0
    )
    timestamp = datetime.now().isoformat(sep=" ", timespec="microseconds")
    base_output_folder = os.path.splitext(args.jsonl_file)[0]
    report_file_path = f"{base_output_folder}_report.txt"
    output_data = data_list.copy()
    try:
        with open(report_file_path, "w") as report_file:
            report_file.write("--------------------------------------------------\n")
            report_file.write(
                "BIRD CRITIC Stack Overflow Result Statistics (Postgres):\n"
            )
            report_file.write(f"Number of Instances: {len(data_list)}\n")
            report_file.write(
                f"Number of Execution Errors: {number_of_execution_errors}\n"
            )
            report_file.write(f"Number of Timeouts: {number_of_timeouts}\n")
            report_file.write(
                f"Number of Assertion Errors: {number_of_assertion_errors}\n"
            )
            report_file.write(
                f"Number of Error SQL Errors: {number_of_error_sql_errors}\n"
            )
            report_file.write(f"Total Errors: {total_errors}\n")
            report_file.write(f"Overall Accuracy: {overall_accuracy:.2f}%\n")
            report_file.write(f"Timestamp: {timestamp}\n\n")

            for i, q_res in enumerate(question_test_case_results):
                q_idx = q_res["instance_id"]
                t_total = q_res["total_test_cases"]
                t_pass = q_res["passed_test_cases"]
                t_fail = t_total - t_pass
                failed_list_str = (
                    ", ".join(q_res["failed_test_cases"]) if t_fail > 0 else "None"
                )
                error_phase_note = (
                    " | Error Phase: Unexpected Pass"
                    if q_res.get("error_phase_unexpected_pass")
                    else ""
                )
                sol_phase_note = (
                    " | Sol Phase: Execution Error"
                    if q_res.get("solution_phase_execution_error")
                    else ""
                )
                sol_phase_note += (
                    " | Sol Phase: Timeout Error"
                    if q_res.get("solution_phase_timeout_error")
                    else ""
                )
                report_file.write(
                    f"Question_{q_idx}: ({t_pass}/{t_total}) test cases passed, "
                    f"failed test cases: {failed_list_str}{error_phase_note}{sol_phase_note}\n"
                )
                output_data[i]["status"] = (
                    "success" if t_fail == 0 and not error_phase_note else "failed"
                )
                if t_fail == 0 and not error_phase_note:
                    output_data[i]["error_message"] = None
                elif error_phase_note:
                    output_data[i][
                        "error_message"
                    ] = "Error Phase: Error SQL did not raise an error, and test cases unexpectedly passed."
                elif failed_list_str:
                    output_data[i]["error_message"] = failed_list_str + " failed"
                else:
                    output_data[i]["error_message"] = sol_phase_note
    except Exception as e:
        print(f"Failed to write report: {e}")

    print("Overall report generated:", report_file_path)

    output_jsonl_file = f"{base_output_folder}_output_with_status.jsonl"
    with open(output_jsonl_file, "w") as f:
        for data in output_data:
            f.write(json.dumps(data) + "\n")

    try:
        close_all_postgresql_pools()
    except Exception as e:
        print(f"Failed to close all PostgreSQL pools: {e}")


if __name__ == "__main__":
    main()
