#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wrapper script to run Oracle evaluation instances in parallel using multiple threads.
This prevents database connection issues or OOM issues from terminating the entire evaluation.
Includes synchronization mechanisms to prevent database template conflicts.
"""
import signal
import argparse
import json
import sys
import os
import multiprocessing
import tempfile
import time
import gc
import threading
import queue
from datetime import datetime
import tqdm
from oracle_utils import (
    create_ephemeral_users,
    drop_ephemeral_users,
    generate_category_report,
    generate_report_and_output,
    cleanup_all_ephemeral_users,
)
from oracle_test_utils import load_jsonl
from logger import configure_logger

# Global dictionary to store database locks
db_template_locks = {}
# Lock to protect access to the db_template_locks dictionary
template_locks_lock = threading.Lock()


def get_db_lock(db_name):
    """Get lock for the specified database, create if it doesn't exist"""
    with template_locks_lock:
        if db_name not in db_template_locks:
            db_template_locks[db_name] = threading.Lock()
        return db_template_locks[db_name]


def run_instance(instance_data, instance_id, args, thread_idx, ephemeral_user):
    """Run a single evaluation instance in a separate process"""
    tmp_input = None
    tmp_output = None

    try:
        # Create temporary input file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as tmp:
            tmp_input = tmp.name
            json.dump(instance_data, tmp)

        # Create temporary output file
        tmp_output = tempfile.NamedTemporaryFile(suffix=".json", delete=False).name

        # Create log directory in the same location as the input file
        log_dir = os.path.dirname(os.path.abspath(args.jsonl_file))
        instance_log_file = os.path.join(log_dir, f"instance_{instance_id}.log")

        # Build command to run the single instance evaluation script
        cmd = [
            "python",
            os.path.join(os.path.dirname(__file__), "single_instance_eval_oracle.py"),
            "--jsonl_file",
            tmp_input,
            "--output_file",
            tmp_output,
            "--mode",
            args.mode,
            "--logging",
            args.logging,
            "--log_file",
            instance_log_file,  # Pass the full log file path
            "--ephemeral_user",
            ephemeral_user,
        ]

        # Get the corresponding database template lock
        db_name = instance_data.get("db_id", "unknown").upper()
        db_lock = get_db_lock(db_name)

        print(
            f"[Thread {thread_idx}] Running instance {instance_id} with ephemeral user {ephemeral_user}..."
        )

        # Use lock to ensure the same template isn't used to create multiple databases simultaneously
        with db_lock:
            print(
                f"[Thread {thread_idx}] Acquired lock for database {db_name}, running process..."
            )
            try:
                # Run the process with timeout
                import subprocess

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=300,  # 5 minute timeout per instance
                )
                success = result.returncode == 0
                if not success:
                    print(
                        f"[Thread {thread_idx}] Instance {instance_id} process returned error code {result.returncode}"
                    )
                    print(f"[Thread {thread_idx}] STDOUT: {result.stdout[:500]}...")
                    print(f"[Thread {thread_idx}] STDERR: {result.stderr[:500]}...")
            except subprocess.TimeoutExpired:
                print(
                    f"[Thread {thread_idx}] Instance {instance_id} timed out after 300 seconds"
                )
                success = False

            # Add a short delay to ensure database operations are fully completed
            time.sleep(1)

        # Lock has been released, process results
        print(
            f"[Thread {thread_idx}] Released lock for database {db_name}, processing results..."
        )

        # If successful, read output, otherwise create failure result
        if success and os.path.exists(tmp_output) and os.path.getsize(tmp_output) > 0:
            try:
                with open(tmp_output, "r") as f:
                    evaluation_result = json.load(f)
                    # Add instance_id to ensure correct sorting later
                    evaluation_result["instance_id"] = instance_id
                    return evaluation_result
            except Exception as e:
                print(
                    f"[Thread {thread_idx}] Error reading output for instance {instance_id}: {e}"
                )

        # If any step fails, return a failure result
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": "Failed to evaluate instance (process error)",
            "total_test_cases": len(instance_data.get("test_cases", [])),
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "solution_phase_execution_error": True,
            "solution_phase_timeout_error": False,
            "solution_phase_assertion_error": False,
        }

    except Exception as e:
        print(f"[Thread {thread_idx}] Unexpected error in run_instance: {e}")
        return {
            "instance_id": instance_id,
            "status": "failed",
            "error_message": f"Unexpected error: {str(e)}",
            "total_test_cases": len(instance_data.get("test_cases", [])),
            "passed_test_cases": 0,
            "failed_test_cases": [],
            "solution_phase_execution_error": True,
            "solution_phase_timeout_error": False,
            "solution_phase_assertion_error": False,
        }
    finally:
        # Ensure temporary files are always cleaned up
        if tmp_input and os.path.exists(tmp_input):
            try:
                os.unlink(tmp_input)
            except Exception as e:
                print(f"[Thread {thread_idx}] Error removing input temp file: {e}")

        if tmp_output and os.path.exists(tmp_output):
            try:
                os.unlink(tmp_output)
            except Exception as e:
                print(f"[Thread {thread_idx}] Error removing output temp file: {e}")


def process_queue(
    work_queue, ephemeral_users, results_dict, global_stats_lock, args, thread_idx
):
    """Worker function to process items from the queue"""
    while True:
        try:
            # Get the next item from the queue
            original_idx, instance_data = work_queue.get(timeout=1)
            instance_id = instance_data.get("instance_id", f"instance_{original_idx}")

            # Get database name for this instance
            db_name = instance_data.get("db_id", "unknown").upper()

            # Get an ephemeral user for this database
            ephemeral_user = None
            try:
                if db_name in ephemeral_users:
                    ephemeral_user = ephemeral_users[db_name].get(timeout=60)
                else:
                    # No ephemeral users for this database
                    with global_stats_lock:
                        results_dict[instance_id] = {
                            "instance_id": instance_id,
                            "status": "failed",
                            "error_message": f"No ephemeral users available for database {db_name}",
                            "total_test_cases": len(
                                instance_data.get("test_cases", [])
                            ),
                            "passed_test_cases": 0,
                            "failed_test_cases": [],
                            "solution_phase_execution_error": True,
                            "solution_phase_timeout_error": False,
                            "solution_phase_assertion_error": False,
                        }
                    work_queue.task_done()
                    continue
            except queue.Empty:
                # No ephemeral user available, store error result and continue
                with global_stats_lock:
                    results_dict[instance_id] = {
                        "instance_id": instance_id,
                        "status": "failed",
                        "error_message": f"No ephemeral user available for database {db_name} (timeout)",
                        "total_test_cases": len(instance_data.get("test_cases", [])),
                        "passed_test_cases": 0,
                        "failed_test_cases": [],
                        "solution_phase_execution_error": True,
                        "solution_phase_timeout_error": False,
                        "solution_phase_assertion_error": False,
                    }
                work_queue.task_done()
                continue

            # Process the instance
            try:
                result = run_instance(
                    instance_data, instance_id, args, thread_idx, ephemeral_user
                )
                with global_stats_lock:
                    results_dict[instance_id] = result
            finally:
                # Always return the ephemeral user to the queue
                if ephemeral_user and db_name in ephemeral_users:
                    ephemeral_users[db_name].put(ephemeral_user)

            # Mark the task as done
            work_queue.task_done()

        except queue.Empty:
            # No more items in the queue
            break
        except Exception as e:
            print(f"[Thread {thread_idx}] Error processing queue item: {e}")
            # Still mark the task as done to avoid hanging
            try:
                work_queue.task_done()
            except:
                pass


def main():
    parser = argparse.ArgumentParser(
        description="Wrapper script to run Oracle evaluation cases using multiple threads."
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
        "--skip",
        type=int,
        default=0,
        help="Skip the first N instances.",
    )
    parser.add_argument(
        "--num_threads",
        type=int,
        default=2,
        help="Number of instances to process in parallel.",
    )
    parser.add_argument(
        "--logging",
        type=str,
        default="false",
        help="Enable or disable per-instance logging ('true' or 'false').",
    )
    parser.add_argument(
        "--mode",
        choices=["gold", "pred"],
        default="pred",
        help="Which field to use for solution SQL (gold or pred).",
    )
    parser.add_argument(
        "--report",
        type=str,
        default="true",
        help="If 'true', generates an additional difficulty-level performance report.",
    )

    args = parser.parse_args()

    # Load data
    data_list = load_jsonl(args.jsonl_file)
    if not data_list:
        print("No data found in the JSONL file.")
        sys.exit(1)

    # Apply skip and limit
    if args.skip > 0:
        data_list = data_list[args.skip :]
    if args.limit is not None:
        data_list = data_list[: args.limit]

    # Setup logger
    base_output_folder = os.path.splitext(args.jsonl_file)[0]
    log_filename = f"{base_output_folder}_wrapper.log"
    logger = configure_logger(log_filename)
    logger.info(
        f"=== Starting Oracle Evaluation via Wrapper Script (Multithreaded with DB locking) ==="
    )
    logger.info(
        f"Processing {len(data_list)} instances from {args.jsonl_file} using {args.num_threads} threads"
    )

    # Add signal handler for graceful cleanup
    def cleanup_handler(signum, frame):
        logger.info("Received termination signal. Starting cleanup...")
        try:
            cleanup_all_ephemeral_users(logger)
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        finally:
            sys.exit(1)

    signal.signal(signal.SIGINT, cleanup_handler)
    signal.signal(signal.SIGTERM, cleanup_handler)

    try:
        # Ensure num_threads is at least 1
        num_threads = max(1, min(args.num_threads, len(data_list)))

        # Collect all needed database names
        all_databases = set(
            item.get("db_id", "unknown").upper()
            for item in data_list
            if "db_id" in item
        )

        # Create ephemeral users for each database
        logger.info(f"Creating ephemeral users for databases: {all_databases}")
        ephemeral_pool = create_ephemeral_users(all_databases, args.num_threads, logger)

        # Create queues for ephemeral users, to facilitate parallel processing
        ephemeral_queues = {}
        for db_name, users in ephemeral_pool.items():
            q = queue.Queue()
            for user in users:
                q.put(user)
            ephemeral_queues[db_name] = q

        try:
            # Create dictionary to store results, using instance_id as keys
            results_dict = {}
            global_stats_lock = threading.Lock()

            # Create work queue
            work_queue = queue.Queue()
            for i, data in enumerate(data_list):
                work_queue.put((i, data))

            # Start worker threads
            threads = []
            for i in range(num_threads):
                thread = threading.Thread(
                    target=process_queue,
                    args=(
                        work_queue,
                        ephemeral_queues,
                        results_dict,
                        global_stats_lock,
                        args,
                        i,
                    ),
                )
                thread.daemon = True
                thread.start()
                threads.append(thread)

            # Show progress bar
            with tqdm.tqdm(total=len(data_list), desc="Evaluating instances") as pbar:
                completed = 0
                while completed < len(data_list):
                    current_completed = len(results_dict)
                    if current_completed > completed:
                        pbar.update(current_completed - completed)
                        completed = current_completed
                    time.sleep(0.1)

                    # Check if all threads have terminated
                    if all(not t.is_alive() for t in threads) and completed < len(
                        data_list
                    ):
                        logger.error(
                            "All threads have terminated but not all instances were processed"
                        )
                        break

            # Wait for all work to complete
            work_queue.join()

            # Stop all threads
            for thread in threads:
                thread.join(timeout=1)

            # Ensure all instances have results
            for i, data in enumerate(data_list):
                instance_id = data.get("instance_id", f"instance_{i}")
                if instance_id not in results_dict:
                    logger.error(f"Missing result for instance {instance_id}")
                    results_dict[instance_id] = {
                        "instance_id": instance_id,
                        "status": "failed",
                        "error_message": "Result missing from processing",
                        "solution_phase_execution_error": True,
                        "total_test_cases": len(data.get("test_cases", [])),
                        "passed_test_cases": 0,
                        "failed_test_cases": [],
                    }

            # Organize results in original list order
            results = []
            for i, data in enumerate(data_list):
                instance_id = data.get("instance_id", f"instance_{i}")
                if instance_id in results_dict:
                    results.append(results_dict[instance_id])

            # Count statistics
            number_of_execution_errors = sum(
                1 for r in results if r.get("solution_phase_execution_error", False)
            )
            number_of_timeouts = sum(
                1 for r in results if r.get("solution_phase_timeout_error", False)
            )
            number_of_assertion_errors = sum(
                1 for r in results if r.get("solution_phase_assertion_error", False)
            )
            number_of_issue_sql_errors = sum(
                1 for r in results if r.get("issue_sql_error", 0) == 1
            )
            number_error_unexpected_pass = sum(
                1 for r in results if r.get("error_phase_unexpected_pass", 0) == 1
            )
            total_passed_instances = sum(
                1 for r in results if r.get("status") == "success"
            )

            # Generate summary report and output file
            generate_report_and_output(
                args.jsonl_file,
                data_list,
                ["" for _ in range(len(data_list))],  # Placeholder for error messages
                results,
                number_of_execution_errors,
                number_of_timeouts,
                number_of_assertion_errors,
                total_passed_instances,
                args.logging == "true",
            )

            # Generate category report if requested
            if args.report == "true":
                report_file_path = f"{base_output_folder}_report.txt"
                model_name = (
                    args.jsonl_file.split("/")[-1]
                    .replace(".jsonl", "")
                    .replace("_final_output", "")
                )
                generate_category_report(
                    results,
                    data_list,
                    report_file_path,
                    logger,
                    model_name=model_name,
                    metric_name="Test Case",
                )
                print(f"Difficulty report generated: {report_file_path}")

            # Output JSONL with status
            output_jsonl_file = f"{base_output_folder}_output_with_status.jsonl"
            with open(output_jsonl_file, "w") as f:
                for i, data in enumerate(data_list):
                    instance_id = data.get("instance_id", f"instance_{i}")
                    for result in results:
                        if result.get("instance_id") == instance_id:
                            data["status"] = result["status"]
                            data["error_message"] = result.get("error_message")
                            f.write(json.dumps(data, ensure_ascii=False) + "\n")
                            break

            # Print summary to console
            print("\nEvaluation Summary:")
            print(f"Total instances: {len(results)}")
            print(f"Passed instances: {total_passed_instances}")
            print(f"Failed instances: {len(results) - total_passed_instances}")
            print(f"Execution errors: {number_of_execution_errors}")
            print(f"Timeouts: {number_of_timeouts}")
            print(f"Assertion errors: {number_of_assertion_errors}")
            print(
                f"Overall accuracy: {(total_passed_instances / len(results) * 100):.2f}%"
            )

        except Exception as e:
            logger.error(f"Error during evaluation: {e}")
            raise

        finally:
            # Clean up database users
            logger.info("Starting final cleanup...")
            try:
                # Drop ephemeral users
                drop_ephemeral_users(ephemeral_pool, logger)
                cleanup_all_ephemeral_users(logger)
                logger.info("All ephemeral users have been dropped.")
            except Exception as e:
                logger.error(f"Error during final database cleanup: {e}")
                # Try emergency cleanup
                try:
                    cleanup_all_ephemeral_users(logger, force=True)
                except Exception as cleanup_error:
                    logger.error(f"Emergency cleanup also failed: {cleanup_error}")

    except Exception as main_error:
        logger.error(f"Fatal error in main: {main_error}")
        # Last attempt to clean up
        try:
            cleanup_all_ephemeral_users(logger, force=True)
        except:
            pass
        raise

    logger.info(
        "=== Oracle Evaluation via Wrapper Script (Multithreaded with DB locking) Completed ==="
    )


if __name__ == "__main__":
    main()
