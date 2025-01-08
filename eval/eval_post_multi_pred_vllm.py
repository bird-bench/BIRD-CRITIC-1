#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evaluation Script for SQL Debugging Test Cases (PostgreSQL Edition)

Multithreaded + Multiple DB Replicas + tqdm Progress Bar + Cleanup All Ephemeral Databases at the End
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
import queue  # For distributing database names
import tqdm
import io
import psycopg2
from psycopg2 import OperationalError
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from test_utils import check_sql_function_usage, remove_distinct, preprocess_results

# Ensure the path to postgresql_setup is correct
sys.path.append("/app/SO_evaluation")
from python_scripts.postgresql_setup import (
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
    直接通过模板库快速重置数据库：
    1) 关闭连接池
    2) 终止连接
    3) dropdb
    4) createdb --template=xxx_template
    """
    if logger is None:
        logger = PrintLogger()
    try:
        pg_host = "bird_critic_postgresql"
        pg_port = 5432
        pg_user = "root"

        env_vars = os.environ.copy()
        env_vars["PGPASSWORD"] = pg_password
        base_db_name = db_name.split("_process_")[0]
        template_db_name = f"{base_db_name}_template"

        logger.info(f"Resetting database {db_name} using template {template_db_name}")

        # 1) 关闭连接池
        logger.info(f"Closing connection pool for database {db_name} before resetting.")
        close_postgresql_pool(db_name)

        # 2) 终止目标数据库的所有连接
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

        # 3) dropdb
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

        # 4) createdb --template=xxx_template
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
    """
    Retrieve the specified field from the data dictionary and split it based on [split].
    """
    field_value = data.get(field_name, "")
    if not field_value:
        return []
    if isinstance(field_value, str):
        # Use [split] as the delimiter with optional surrounding whitespace
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


# --- NEW: Get a dedicated connection for a phase ---
def get_connection_for_phase(db_name, logger):
    """
    Acquire a new connection (borrowed from the connection pool) for a specific phase.
    """
    logger.info(f"Acquiring dedicated connection for phase on db: {db_name}")
    # Execute a small query to obtain the connection
    _, conn = perform_query_on_postgresql_databases("SELECT 1", db_name, conn=None)
    return conn


class NullLogger:
    """A Logger implementation that does not output any logs"""

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
    queries, db_name, conn, logger=None, section_title="", is_solution=True
):
    """
    Execute a list of queries using the SAME connection (conn).
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
            logger.error(f"Timeout error executing query {i+1}: {e}")
            if is_solution:
                timeout_error = True
        except OperationalError as e:
            logger.error(f"OperationalError executing query {i+1}: {e}")
            if is_solution:
                execution_error = True
        except psycopg2.Error as e:
            logger.error(f"psycopg2 Error executing query {i+1}: {e}")
            if is_solution:
                execution_error = True
        except subprocess.TimeoutExpired as e:
            logger.error(f"Subprocess timeout executing query {i+1}: {e}")
            if is_solution:
                timeout_error = True
        except Exception as e:
            logger.error(f"Generic error executing query {i+1}: {e}")
            if is_solution:
                execution_error = True
        finally:
            logger.info(f"[{section_title}] DB: {db_name}, conn info: {conn}")

    log_section_footer(logger)
    return query_result, execution_error, timeout_error


# --- CHANGE: Use the same conn without internal close ---
def execute_error_sql(error_sql_list, db_name, logger, conn):
    """
    Execute a list of SQL statements that are expected to produce errors, using the SAME conn.
    Returns the first encountered error_message, and the result if no error happened.
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
                logger.error(f"Timeout error executing error SQL {i+1}: {e}")
                error_message = str(e)
                break
            except psycopg2.Error as e:
                logger.info(f"Expected error encountered for SQL {i+1}: {e}")
                error_message = str(e)
                break
            except subprocess.TimeoutExpired as e:
                logger.error(f"Subprocess timeout executing error SQL {i+1}: {e}")
                error_message = f"Timeout: {str(e)}"
                break
            except Exception as e:
                logger.info(f"Expected error encountered for SQL {i+1}: {e}")
                error_message = str(e)
                break
            finally:
                logger.info(f"[Error SQL] DB: {db_name}, conn: {conn}")
    log_section_footer(logger)
    return error_message, error_sql_result


def run_test_case(
    test_code, result, logger, idx, return_dict, conn, error_sql, sol_sql, db_name
):
    # 1. Prepare global_env and local_env
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

    # Construct the test case code
    test_case_code = "from datetime import date\n" + test_code
    test_case_code += (
        "\n__test_case_result__ = test_case(pred_sqls, sol_sqls, db_name, conn)"
    )

    logger.info(f"Test case content:\n{test_case_code}")
    logger.info(f"Executing test case {idx}")

    # 2. Redirect sys.stdout to StringIO to capture prints from test_code
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
        # 3. Restore sys.stdout
        sys.stdout = old_stdout

    # 4. Log the captured stdout
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
        p.join(timeout=60)  # Each test case has a maximum of 60 seconds
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


# --- CHANGE: Preprocessing should also use the same conn ---
def run_preprocessing(preprocess_sql, db_name, logger, conn):
    if preprocess_sql:
        execute_queries(
            preprocess_sql,
            db_name,
            conn,
            logger,
            section_title="Preprocess SQL",
            is_solution=False,
        )


def run_error_phase(error_sql, sol_sql, db_name, test_cases, logger, conn, efficiency):
    """
    1. Execute error queries (expected to fail) with the given conn.
    2. If no error occurs, run test cases that are expected to fail.
    """
    error_message, error_sql_result = execute_error_sql(
        error_sql, db_name, logger, conn
    )
    assertion_error = False

    # If no error was triggered, execute test_cases (which should fail)
    if error_message is None and test_cases and not efficiency:
        passed_count, failed_tests = execute_test_cases(
            test_cases, error_sql_result, logger, conn, error_sql, sol_sql, db_name
        )
        if failed_tests:
            assertion_error = False  # They did fail as expected
        else:
            assertion_error = True  # They unexpectedly passed
    return error_message, error_sql_result, assertion_error


def run_solution_phase(
    pred_sql, gold_sql, error_sql, db_name, test_cases, logger, conn, efficiency
):
    """
    1. Execute solution queries using the given conn.
    2. Run test cases on the solution results if no major error/timeout.
    """
    sol_sql_result, exec_error_flag, timeout_flag = execute_queries(
        pred_sql,
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
            passed_count, failed_tests = execute_test_cases(
                test_cases, sol_sql_result, logger, conn, pred_sql, gold_sql, db_name
            )
        else:
            passed_count, failed_tests = execute_test_cases(
                test_cases, sol_sql_result, logger, conn, error_sql, pred_sql, db_name
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


def create_ephemeral_db_copies(base_db_names, num_copies, pg_password, logger):
    """
    For each base_db (e.g., 'financial', 'formula_1'), create num_copies ephemeral databases
    (e.g., financial_process_1, financial_process_2, ...), each from base_db_template.
    Returns a dictionary: { 'financial': ['financial_process_1', ...], 'formula_1': [...] }
    """
    pg_host = "bird_critic_postgresql"
    pg_port = 5432
    pg_user = "root"
    env_vars = os.environ.copy()
    env_vars["PGPASSWORD"] = pg_password

    ephemeral_db_pool = {}

    for base_db in base_db_names:
        base_template = f"{base_db}_template"
        ephemeral_db_pool[base_db] = []

        for i in range(1, num_copies + 1):
            ephemeral_name = f"{base_db}_process_{i}"
            # If it already exists, drop it first
            drop_cmd = [
                "dropdb",
                "--if-exists",
                "-h",
                pg_host,
                "-p",
                str(pg_port),
                "-U",
                pg_user,
                ephemeral_name,
            ]
            subprocess.run(
                drop_cmd,
                check=False,
                env=env_vars,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # createdb --template
            create_cmd = [
                "createdb",
                "-h",
                pg_host,
                "-p",
                str(pg_port),
                "-U",
                pg_user,
                ephemeral_name,
                "--template",
                base_template,
            ]
            logger.info(
                f"Creating ephemeral db {ephemeral_name} from {base_template}..."
            )
            subprocess.run(
                create_cmd,
                check=True,
                env=env_vars,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            ephemeral_db_pool[base_db].append(ephemeral_name)

        logger.info(
            f"For base_db={base_db}, ephemeral db list = {ephemeral_db_pool[base_db]}"
        )

    return ephemeral_db_pool


def drop_ephemeral_dbs(ephemeral_db_pool_dict, pg_password, logger):
    """
    Delete all ephemeral databases created during the script execution.
    """
    pg_host = "bird_critic_postgresql"
    pg_port = 5432
    pg_user = "root"
    env_vars = os.environ.copy()
    env_vars["PGPASSWORD"] = pg_password

    logger.info("=== Cleaning up ephemeral databases ===")
    for base_db, ephemeral_list in ephemeral_db_pool_dict.items():
        for ephemeral_db in ephemeral_list:
            logger.info(f"Dropping ephemeral db: {ephemeral_db}")
            drop_cmd = [
                "dropdb",
                "--if-exists",
                "-h",
                pg_host,
                "-p",
                str(pg_port),
                "-U",
                pg_user,
                ephemeral_db,
            ]
            try:
                subprocess.run(
                    drop_cmd,
                    check=True,
                    env=env_vars,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to drop ephemeral db {ephemeral_db}: {e}")


def ex_base(pred_sqls, sol_sqls, db_name, conn):
    """
    Execute predicted SQL list and ground truth SQL list, and compare if the results are identical.
    Returns 1 if identical, otherwise 0.
    """
    # If either list is empty, return 0
    if not pred_sqls or not sol_sqls:
        return 0

    # Result comparison function
    def calculate_ex(predicted_res, ground_truth_res):
        # Compare using sets to ignore order and duplicates
        return 1 if set(predicted_res) == set(ground_truth_res) else 0

    # Execute predicted SQL list
    predicted_res, pred_execution_error, pred_timeout_error = execute_queries(
        pred_sqls, db_name, conn, None, "", True
    )

    # Execute ground truth SQL list
    ground_truth_res, gt_execution_error, gt_timeout_error = execute_queries(
        sol_sqls, db_name, conn, None, "", True
    )

    # If any execution or timeout error occurs, return 0
    if (
        gt_execution_error
        or gt_timeout_error
        or pred_execution_error
        or pred_timeout_error
    ):
        return 0

    # If results are None or empty, decide based on requirements (here, return 0)
    if not predicted_res or not ground_truth_res:
        return 0
    predicted_res = preprocess_results(predicted_res)
    ground_truth_res = preprocess_results(ground_truth_res)
    # If both results are successfully retrieved, compare them
    return calculate_ex(predicted_res, ground_truth_res)


def performance_compare_by_qep(old_sqls, sol_sqls, db_name, conn):
    """
    Compare total plan cost of old_sqls vs. sol_sqls in one connection,
    by using transactions + ROLLBACK to ensure each group sees the same initial state.

    Returns 1 if sol_sqls total plan cost is lower, otherwise 0.

    Notes:
      - If old_sqls / sol_sqls contain schema changes or data modifications,
        we rely on transaction rollback to discard those changes before measuring the other side.
      - EXPLAIN itself does not execute the query, only returns the plan and cost estimate.
      - This approach will not reflect persistent changes made by old_sqls if they are needed by sol_sqls.
        Instead, it ensures both sets see the same starting state for cost comparison.
    """

    if not old_sqls or not sol_sqls:
        print("Either old_sqls or sol_sqls is empty. Returning 0.")
        return 0
    print(f"Old SQLs are {old_sqls}")
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

    # --- Measure cost for old_sqls ---
    try:
        # Start a transaction
        perform_query_on_postgresql_databases("BEGIN", db_name, conn=conn)
        old_total_cost = measure_sqls_cost(old_sqls)
        print(f"Old SQLs total plan cost: {old_total_cost}")
    finally:
        # Always rollback so that old_sqls changes are not visible to sol_sqls
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


def process_one_instance(data_item, ephemeral_db_queues, args, global_stats_lock):
    global number_of_execution_errors, number_of_timeouts
    global number_of_assertion_errors, number_of_error_sql_errors
    global total_passed_instances, number_error_unexpected_pass

    instance_id = data_item["instance_id"]
    log_filename = os.path.splitext(args.jsonl_file)[0] + f"_instance_{instance_id}.log"
    if args.logging == "true":
        logger = configure_logger(log_filename)
    else:
        logger = NullLogger()
    required_fields = ["selected_database", "preprocess_sql", "error_sql", "sol_sql"]
    missing_fields = [field for field in required_fields if field not in data_item]
    if missing_fields:
        logger.error(f"Missing required fields: {', '.join(missing_fields)}")
        with global_stats_lock:
            number_of_error_sql_errors += 1
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": f"Missing fields: {', '.join(missing_fields)}",
            "total_test_cases": len(data_item.get("test_cases", [])),
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "error_phase_unexpected_pass": 0,
            "solution_phase_execution_error": False,
            "solution_phase_timeout_error": False,
            "solution_phase_assertion_error": False,
        }

    efficiency = data_item.get("efficiency", False)
    db_name = data_item["selected_database"]
    preprocess_sql = split_field(data_item, "preprocess_sql")
    error_sql = split_field(data_item, "error_sql")
    gold_sql = None
    if args.mode == "gold":
        pred_sql = split_field(data_item, "sol_sql")
    else:
        pred_sql = split_field(data_item, "pred_sqls")
        gold_sql = split_field(data_item, "sol_sql")

    clean_up_sql = split_field(data_item, "clean_up_sql")
    test_cases = data_item.get("test_cases", [])
    language = data_item.get("language", "postgresql")

    error_phase_unexpected_pass = 0
    solution_phase_execution_error = False
    solution_phase_timeout_error = False
    solution_phase_assertion_error = False

    total_test_cases = len(test_cases)
    passed_test_cases_count = 0
    failed_test_cases = []
    error_message_text = ""

    # Get an ephemeral database from the queue
    try:
        ephemeral_db = ephemeral_db_queues[db_name].get(
            timeout=60
        )  # Wait up to 60 seconds
    except queue.Empty:
        logger.error(f"No available ephemeral databases for base_db: {db_name}")
        with global_stats_lock:
            number_of_execution_errors += 1
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": "No available ephemeral databases.",
            "total_test_cases": total_test_cases,
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "error_phase_unexpected_pass": 0,
            "solution_phase_execution_error": True,
            "solution_phase_timeout_error": False,
            "solution_phase_assertion_error": False,
        }

    logger.info(f"Instance {instance_id} is using ephemeral db: {ephemeral_db}")

    try:
        # Phase 1: Error scenario
        logger.info("=== Starting Error Phase ===")
        reset_and_restore_database(ephemeral_db, args.pg_password, logger)

        # Acquire connection for error phase
        error_conn = get_connection_for_phase(ephemeral_db, logger)

        run_preprocessing(preprocess_sql, ephemeral_db, logger, error_conn)

        err_msg, error_sql_result, err_assertion = run_error_phase(
            error_sql,
            gold_sql,
            ephemeral_db,
            test_cases,
            logger,
            error_conn,
            efficiency,
        )
        if err_msg:
            error_message_text += err_msg

        # Close error_conn
        close_postgresql_connection(ephemeral_db, error_conn)
        if err_msg is None and err_assertion:
            logger.info(
                "Error SQL did not raise an error, and test cases unexpectedly passed."
            )
            error_phase_unexpected_pass = 1
        elif err_msg is None and not err_assertion:
            logger.info(
                "Error SQL did not raise an error, but test cases failed as expected."
            )

        logger.info("=== Error Phase Completed ===")

        # Phase 2: Solution scenario
        logger.info("=== Starting Solution Phase ===")
        reset_and_restore_database(ephemeral_db, args.pg_password, logger)

        # Acquire connection for solution phase
        solution_conn = get_connection_for_phase(ephemeral_db, logger)

        run_preprocessing(preprocess_sql, ephemeral_db, logger, solution_conn)

        (
            sol_exec_err,
            sol_timeout_err,
            sol_assert_err,
            passed_count,
            failed_tests_phase2,
        ) = run_solution_phase(
            pred_sql,
            gold_sql,
            error_sql,
            ephemeral_db,
            test_cases,
            logger,
            solution_conn,
            efficiency,
        )

        # Close solution_conn
        close_postgresql_connection(ephemeral_db, solution_conn)

        solution_phase_execution_error = sol_exec_err
        solution_phase_timeout_error = sol_timeout_err
        solution_phase_assertion_error = sol_assert_err

        passed_test_cases_count += passed_count
        failed_test_cases.extend(failed_tests_phase2)

        # Cleanup SQL
        if clean_up_sql:
            logger.info("Executing Clean Up SQL after solution phase.")
            new_temp_conn = get_connection_for_phase(ephemeral_db, logger)
            execute_queries(
                clean_up_sql,
                ephemeral_db,
                new_temp_conn,
                logger,
                section_title="Clean Up SQL",
                is_solution=False,
            )
            close_postgresql_connection(ephemeral_db, new_temp_conn)

        logger.info("=== Solution Phase Completed ===")

    except Exception as e:
        logger.error(f"Error during execution for question {instance_id}: {e}")
        solution_phase_execution_error = True
        error_message_text += str(e)

    finally:
        # Return the ephemeral database back to the queue
        ephemeral_db_queues[db_name].put(ephemeral_db)
        logger.info(
            f"Instance {instance_id} finished. Returned ephemeral db: {ephemeral_db}"
        )

    # Update global statistics with thread-safe lock
    with global_stats_lock:
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

    # Determine the status based on error flags
    ret_status = "success"
    if (
        error_phase_unexpected_pass
        or solution_phase_execution_error
        or solution_phase_timeout_error
        or solution_phase_assertion_error
    ):
        ret_status = "failed"

    return {
        "instance_id": instance_id,
        "status": ret_status,
        "error_message": error_message_text if error_message_text else None,
        "total_test_cases": total_test_cases,
        "passed_test_cases": passed_test_cases_count,
        "failed_test_cases": failed_test_cases,
        "error_phase_unexpected_pass": error_phase_unexpected_pass,
        "solution_phase_execution_error": solution_phase_execution_error,
        "solution_phase_timeout_error": solution_phase_timeout_error,
        "solution_phase_assertion_error": solution_phase_assertion_error,
    }


def main():
    # ====== Declare global variables to modify ======
    global number_of_execution_errors, number_of_timeouts
    global number_of_assertion_errors, number_of_error_sql_errors
    global total_passed_instances, number_error_unexpected_pass
    global question_test_case_results

    parser = argparse.ArgumentParser(
        description="Execute SQL solution and test cases (PostgreSQL)."
    )
    parser.add_argument(
        "--jsonl_file",
        required=True,
        help="Path to the JSONL file containing the dataset instances.",
    )
    parser.add_argument(
        "--pg_password",
        default="123123",
        help="PostgreSQL password for resetting the database.",
    )
    parser.add_argument(
        "--mode",
        choices=["gold", "pred"],
        default="pred",
        help="Which field to use for solution SQL (gold or pred).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of instances to process.",
    )
    parser.add_argument(
        "--num_threads", type=int, default=4, help="Number of parallel threads to use."
    )
    # NEW: --logging argument
    parser.add_argument(
        "--logging",
        type=str,
        default="false",
        help="Enable or disable per-instance logging ('true' or 'false'). Default is 'true'.",
    )
    args = parser.parse_args()

    data_list = load_jsonl(args.jsonl_file)
    if not data_list:
        print("No data found in the JSONL file.")
        sys.exit(1)

    if args.limit is not None:
        data_list = data_list[: args.limit]
    # Remove the following line to respect the limit
    # data_list = data_list[:10]

    # Collect all base database names
    all_db_names = set()
    for d in data_list:
        if "selected_database" in d:
            all_db_names.add(d["selected_database"])

    # Create a summary log
    base_output_folder = os.path.splitext(args.jsonl_file)[0]
    big_log_filename = f"{base_output_folder}_multi_thread.log"
    big_logger = configure_logger(big_log_filename)
    big_logger.info(
        f"=== Starting Multi-Thread Evaluation with {args.num_threads} threads ==="
    )

    # Step 1: Create num_threads ephemeral databases for each base_db_name
    ephemeral_db_pool_dict = create_ephemeral_db_copies(
        base_db_names=all_db_names,
        num_copies=args.num_threads,
        pg_password=args.pg_password,
        logger=big_logger,
    )

    # Use queue to manage distribution
    ephemeral_db_queues = {}
    for base_db, ephemeral_list in ephemeral_db_pool_dict.items():
        q = queue.Queue()
        for ep_db in ephemeral_list:
            q.put(ep_db)
        ephemeral_db_queues[base_db] = q

    # Thread-safe lock
    global_stats_lock = threading.Lock()

    # Multithreaded execution with tqdm progress bar
    results = []
    total_instances = len(data_list)

    from tqdm import tqdm as tqdm_progress

    with ThreadPoolExecutor(max_workers=args.num_threads) as executor, tqdm_progress(
        total=total_instances, desc="Evaluating Questions"
    ) as pbar:
        future_to_data = {}
        for data_item in data_list:
            future = executor.submit(
                process_one_instance,
                data_item,
                ephemeral_db_queues,
                args,
                global_stats_lock,
            )
            future_to_data[future] = data_item

        for fut in as_completed(future_to_data):
            res = fut.result()
            results.append(res)
            pbar.update(1)  # Update progress bar after each instance

    question_test_case_results = results
    output_data = data_list.copy()
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
    report_file_path = f"{base_output_folder}_report.txt"

    # Generate the report
    try:
        with open(report_file_path, "w") as report_file:
            report_file.write("--------------------------------------------------\n")
            report_file.write(
                "BIRD CRITIC Stack Overflow Result Statistics (Postgres, Multi-Thread):\n"
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
                sol_phase_note = ""
                if q_res.get("solution_phase_execution_error"):
                    sol_phase_note += " | Sol Phase: Execution Error"
                if q_res.get("solution_phase_timeout_error"):
                    sol_phase_note += " | Sol Phase: Timeout Error"
                if q_res.get("solution_phase_assertion_error"):
                    sol_phase_note += " | Sol Phase: Assertion Error"

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

    # Output JSONL with status
    if args.logging == "true":
        output_jsonl_file = f"{base_output_folder}_output_with_status.jsonl"
        with open(output_jsonl_file, "w") as f:
            for i, data in enumerate(output_data):
                data["status"] = question_test_case_results[i]["status"]
                data["error_message"] = question_test_case_results[i]["error_message"]
                f.write(json.dumps(data) + "\n")

    # Close all PostgreSQL pools
    try:
        close_all_postgresql_pools()
    except Exception as e:
        print(f"Failed to close all PostgreSQL pools: {e}")

    # Finally, delete all ephemeral databases
    drop_ephemeral_dbs(ephemeral_db_pool_dict, args.pg_password, big_logger)
    big_logger.info("All ephemeral databases have been dropped.")


if __name__ == "__main__":
    main()
