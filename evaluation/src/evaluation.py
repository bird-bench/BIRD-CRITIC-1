# evaluation.py
import argparse
import sys
import os
import io
import multiprocessing
import threading
import queue
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tqdm import tqdm as tqdm_progress

# Local imports
from logger import (
    configure_logger,
    NullLogger,
)
from utils import load_jsonl, split_field, save_report_and_status
from db_utils import (
    perform_query_on_postgresql_databases,
    close_postgresql_connection,
    execute_queries,
    close_all_postgresql_pools,
    get_connection_for_phase,
    reset_and_restore_database,
    create_ephemeral_db_copies,
    drop_ephemeral_dbs,
)
from test_utils import (
    check_sql_function_usage,
    remove_distinct,
    preprocess_results,
    ex_base,
    performance_compare_by_qep,
)
from datasets import load_dataset

# Global counters
number_of_execution_errors = 0
number_of_timeouts = 0
number_of_assertion_errors = 0
total_passed_instances = 0
number_error_unexpected_pass = 0
question_test_case_results = []


def run_test_case(
    test_code, result, logger, idx, return_dict, conn, pred_sqls, sol_sqls, db_name
):
    """
    In a separate Process, runs the test_code with the given environment and captures pass/fail status.
    """
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
        "pred_sqls": pred_sqls,
        "sol_sqls": sol_sqls,
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
    """
    Spawns each test case in a separate Process.
    Returns (passed_count, failed_tests).
    """
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
    Execute any pre-processing SQL statements.
    """
    if preprocess_sql:
        execute_queries(
            preprocess_sql, db_name, conn, logger, section_title="Preprocess SQL"
        )


def run_evaluation_phase(
    pred_sqls, sol_sqls, error_sqls, db_name, test_cases, logger, conn, efficiency
):
    """
    1. Execute 'pred_sql'
    2. If no error, run test cases.
    Returns tuple of flags + (passed_count, failed_tests).
    """
    sol_sql_result, exec_error_flag, timeout_flag = execute_queries(
        pred_sqls, db_name, conn, logger, section_title="LLM Generated SQL"
    )

    instance_execution_error = exec_error_flag
    instance_timeout_error = timeout_flag
    instance_assertion_error = False
    passed_count = 0
    failed_tests = []

    if not instance_execution_error and not instance_timeout_error and test_cases:
        if not efficiency:
            passed_count, failed_tests = execute_test_cases(
                test_cases,
                sol_sql_result,
                logger,
                conn,
                pred_sqls,  # pred_sqls param for run_test_case
                sol_sqls,  # sol_sqls param for run_test_case
                db_name,
            )
        else:
            passed_count, failed_tests = execute_test_cases(
                test_cases,
                sol_sql_result,
                logger,
                conn,
                error_sqls,  # pass error_sql as "pred_sqls" slow
                pred_sqls,  # pass pred_sql as "sol_sqls" fast
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
    )


def process_one_instance(data_item, ephemeral_db_queues, args, global_stats_lock):
    """
    Orchestrate the entire logic for a single instance:
      - Acquire ephemeral DB
      - Evaluation Phase
      - Cleanup
      - Update global counters
    """
    global number_of_execution_errors, number_of_timeouts
    global number_of_assertion_errors
    global total_passed_instances, number_error_unexpected_pass

    instance_id = data_item["instance_id"]
    log_filename = os.path.splitext(args.jsonl_file)[0] + f"_instance_{instance_id}.log"

    if args.logging == "true":
        logger = configure_logger(log_filename)
    else:
        logger = NullLogger()

    required_fields = [
        "selected_database",
        "preprocess_sql",
        "error_sql",
        "sol_sql",
        "pred_sqls",
    ]
    missing_fields = [field for field in required_fields if field not in data_item]
    if missing_fields:
        logger.error(f"Missing required fields: {', '.join(missing_fields)}")
        with global_stats_lock:
            number_of_execution_errors += 1
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": f"Missing fields: {', '.join(missing_fields)}",
            "total_test_cases": len(data_item.get("test_cases", [])),
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "evaluation_phase_execution_error": False,
            "evaluation_phase_timeout_error": False,
            "evaluation_phase_assertion_error": False,
        }

    efficiency = data_item.get("efficiency", False)
    db_name = data_item["selected_database"]
    preprocess_sql = split_field(data_item, "preprocess_sql")
    error_sqls = split_field(data_item, "error_sql")
    pred_sqls = split_field(data_item, "pred_sqls")
    sol_sqls = split_field(data_item, "sol_sql")
    clean_up_sql = split_field(data_item, "clean_up_sql")
    test_cases = data_item.get("test_cases", [])

    evaluation_phase_execution_error = False
    evaluation_phase_timeout_error = False
    evaluation_phase_assertion_error = False
    total_test_cases = len(test_cases)
    passed_test_cases_count = 0
    failed_test_cases = []
    error_message_text = ""

    # Acquire ephemeral db
    try:
        ephemeral_db = ephemeral_db_queues[db_name].get(timeout=60)
    except queue.Empty:
        logger.error(f"No available ephemeral databases for base_db: {db_name}")
        with global_stats_lock:
            print("run here")
            number_of_execution_errors += 1
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": "No available ephemeral databases.",
            "total_test_cases": total_test_cases,
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "evaluation_phase_execution_error": True,
            "evaluation_phase_timeout_error": False,
            "evaluation_phase_assertion_error": False,
        }

    logger.info(f"Instance {instance_id} is using ephemeral db: {ephemeral_db}")

    try:

        # ---------- Evaluation Phase ----------
        logger.info("=== Starting Evaluation Phase ===")

        evaluation_conn = get_connection_for_phase(ephemeral_db, logger)
        run_preprocessing(preprocess_sql, ephemeral_db, logger, evaluation_conn)

        (
            evaluation_phase_execution_error,
            evaluation_phase_timeout_error,
            evaluation_phase_assertion_error,
            passed_count,
            failed_tests,
        ) = run_evaluation_phase(
            pred_sqls,
            sol_sqls,
            error_sqls,
            ephemeral_db,
            test_cases,
            logger,
            evaluation_conn,
            efficiency,
        )

        close_postgresql_connection(ephemeral_db, evaluation_conn)

        passed_test_cases_count += passed_count
        failed_test_cases.extend(failed_tests)

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
            )
            close_postgresql_connection(ephemeral_db, new_temp_conn)

        reset_and_restore_database(ephemeral_db, "123123", logger)
        logger.info("=== Evaluation Phase Completed ===")

    except Exception as e:
        print(f"RUN HERE instance {instance_id} with ERROR {e}")
        logger.error(f"Error during execution for question {instance_id}: {e}")
        error_message_text += str(e)

    finally:
        # Return the ephemeral database to the queue
        ephemeral_db_queues[db_name].put(ephemeral_db)
        logger.info(
            f"Instance {instance_id} finished. Returned ephemeral db: {ephemeral_db}"
        )

    # ---------- Update Global Stats ----------
    with global_stats_lock:
        if evaluation_phase_execution_error:
            number_of_execution_errors += 1
        if evaluation_phase_timeout_error:
            number_of_timeouts += 1
        if evaluation_phase_assertion_error:
            number_of_assertion_errors += 1
        if (
            not evaluation_phase_execution_error
            and not evaluation_phase_timeout_error
            and not evaluation_phase_assertion_error
        ):
            total_passed_instances += 1

    # ---------- Determine status ----------
    ret_status = "success"
    if (
        evaluation_phase_execution_error
        or evaluation_phase_timeout_error
        or evaluation_phase_assertion_error
    ):
        ret_status = "failed"

    return {
        "instance_id": instance_id,
        "status": ret_status,
        "error_message": error_message_text if error_message_text else None,
        "total_test_cases": total_test_cases,
        "passed_test_cases": passed_test_cases_count,
        "failed_test_cases": failed_test_cases,
        "evaluation_phase_execution_error": evaluation_phase_execution_error,
        "evaluation_phase_timeout_error": evaluation_phase_timeout_error,
        "evaluation_phase_assertion_error": evaluation_phase_assertion_error,
    }


def main():
    global number_of_execution_errors, number_of_timeouts
    global number_of_assertion_errors
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
        "--limit",
        type=int,
        default=None,
        help="Limit the number of instances to process.",
    )
    parser.add_argument(
        "--num_threads", type=int, default=2, help="Number of parallel threads to use."
    )
    parser.add_argument(
        "--logging",
        type=str,
        default="false",
        help="Enable or disable per-instance logging ('true' or 'false').",
    )
    args = parser.parse_args()

    data_list = load_jsonl(args.jsonl_file)

    # or to load the data from the Hugging Face dataset
    # dataset = load_dataset("birdsql/bird-critic-1.0-flash-exp")
    # data_list = dataset["flash"]

    if not data_list:
        print("No data found in the JSONL file.")
        sys.exit(1)

    if args.limit is not None:
        data_list = data_list[: args.limit]

    # Collect base DB names
    all_db_names = set()
    for d in data_list:
        if "selected_database" in d:
            all_db_names.add(d["selected_database"])

    # summary logger
    base_output_folder = os.path.splitext(args.jsonl_file)[0]
    ephemeral_db_log_filename = f"{base_output_folder}_multi_thread.log"
    ephemeral_db_logger = configure_logger(ephemeral_db_log_filename)
    ephemeral_db_logger.info(
        f"=== Starting Multi-Thread Evaluation with {args.num_threads} threads ==="
    )

    # Create ephemeral DB copies
    ephemeral_db_pool_dict = create_ephemeral_db_copies(
        base_db_names=all_db_names,
        num_copies=args.num_threads,
        pg_password="123123",
        logger=ephemeral_db_logger,
    )

    # Initialize queues
    ephemeral_db_queues = {}
    for base_db, ephemeral_list in ephemeral_db_pool_dict.items():
        q = queue.Queue()
        for ep_db in ephemeral_list:
            q.put(ep_db)
        ephemeral_db_queues[base_db] = q

    global_stats_lock = threading.Lock()
    results = []
    total_instances = len(data_list)

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
            pbar.update(1)

    question_test_case_results = results[:]
    # Summarize results
    total_errors = (
        number_of_execution_errors + number_of_timeouts + number_of_assertion_errors
    )
    overall_accuracy = (
        ((total_instances - total_errors) / total_instances * 100)
        if total_instances > 0
        else 0.0
    )
    timestamp = datetime.now().isoformat(sep=" ", timespec="microseconds")
    report_file_path = f"{base_output_folder}_report.txt"

    # Generate the report + update data_list
    save_report_and_status(
        report_file_path,
        question_test_case_results,
        data_list,
        number_of_execution_errors,
        number_of_timeouts,
        number_of_assertion_errors,
        overall_accuracy,
        timestamp,
        ephemeral_db_logger,
    )

    print("Overall report generated:", report_file_path)

    # If logging enabled, output JSONL with status
    if args.logging == "true":
        output_jsonl_file = f"{base_output_folder}_output_with_status.jsonl"
        with open(output_jsonl_file, "w") as f:
            for i, data in enumerate(data_list):
                data["status"] = question_test_case_results[i]["status"]
                data["error_message"] = question_test_case_results[i]["error_message"]
                f.write(json.dumps(data) + "\n")

    # Close all pools, drop ephemeral DBs
    try:
        close_all_postgresql_pools()
    except Exception as e:
        print(f"Failed to close all PostgreSQL pools: {e}")

    drop_ephemeral_dbs(ephemeral_db_pool_dict, "123123", ephemeral_db_logger)
    ephemeral_db_logger.info("All ephemeral databases have been dropped.")


if __name__ == "__main__":
    main()
