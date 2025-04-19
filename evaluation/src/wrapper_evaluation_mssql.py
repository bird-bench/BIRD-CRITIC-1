#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wrapper script to run MS SQL Server evaluation instances in parallel using multiple threads.
This prevents database connection issues or OOM issues from terminating the entire evaluation.
Includes synchronization mechanisms to prevent database template conflicts.
Enhanced with better resource management and cleanup procedures.
"""

import argparse
import json
import os
import sys
import subprocess
import tempfile
import time
import threading
import signal
import queue
from tqdm import tqdm
from mssql_utils import (
    load_jsonl,
    generate_report_and_output,
    generate_category_report,
    reset_and_restore_database,
)
from logger import configure_logger

# Create a dictionary to store database locks
db_locks = {}
# Create a lock to protect the db_locks dictionary access
locks_lock = threading.Lock()

# Global flag to track cleanup state
cleanup_in_progress = False


def get_db_lock(db_name):
    """Get the lock for the specified database, creating one if it doesn't exist"""
    with locks_lock:
        if db_name not in db_locks:
            db_locks[db_name] = threading.Lock()
        return db_locks[db_name]


def comprehensive_database_cleanup(db_names, logger):
    """Performs a comprehensive cleanup of all databases used in the evaluation"""
    logger.info("Starting comprehensive database cleanup...")
    cleanup_failures = []

    for db_name in db_names:
        try:
            logger.info(f"Resetting and restoring database {db_name}")
            reset_and_restore_database(db_name, logger)
            logger.info(f"Successfully reset database {db_name}")
        except Exception as e:
            error_msg = f"Failed to clean up database {db_name}: {str(e)}"
            logger.error(error_msg)
            cleanup_failures.append(error_msg)

    if cleanup_failures:
        logger.warning(f"Completed cleanup with {len(cleanup_failures)} failures")
        return False
    else:
        logger.info("Comprehensive cleanup completed successfully")
        return True


def emergency_cleanup(db_names, logger):
    """Last resort cleanup procedure for databases"""
    logger.info("Performing emergency cleanup...")

    for db_name in db_names:
        try:
            # Force cleaning by running simpler SQL commands directly
            logger.info(f"Attempting emergency reset of {db_name}")
            reset_and_restore_database(db_name, logger, emergency=True)
        except Exception as e:
            logger.error(f"Emergency cleanup failed for {db_name}: {str(e)}")

    logger.info("Emergency cleanup completed")


def run_instance(instance_data, instance_id, args, idx):
    """Run a single evaluation instance in a separate process"""

    # Get the database name for this instance
    db_name = instance_data.get("db_id", "")
    if not db_name:
        print(f"Warning: Instance {instance_id} has no database specified.")
        db_name = "unknown_db"

    # Create temporary files for input and output
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
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
        os.path.join(os.path.dirname(__file__), "single_instance_eval_mssql.py"),
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
    ]

    # Get the lock for this database
    db_lock = get_db_lock(db_name)

    print(f"[Thread {idx}] Running instance {instance_id} with database {db_name}...")

    # Use the lock to ensure the same database isn't used concurrently
    with db_lock:
        print(
            f"[Thread {idx}] Acquired lock for database {db_name}, running process..."
        )
        try:
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
                    f"[Thread {idx}] Instance {instance_id} process returned error code {result.returncode}"
                )
                print(f"[Thread {idx}] STDOUT: {result.stdout[:500]}...")
                print(f"[Thread {idx}] STDERR: {result.stderr[:500]}...")
        except subprocess.TimeoutExpired:
            print(f"[Thread {idx}] Instance {instance_id} timed out after 300 seconds")
            success = False

        # Add a short delay to ensure database operations are fully completed
        time.sleep(1)

        # Ensure the database is reset before releasing the lock
        try:
            reset_and_restore_database(db_name, logger=None)
        except Exception as e:
            print(
                f"[Thread {idx}] Error resetting database after instance {instance_id}: {e}"
            )
            # Try one more time with a delay
            time.sleep(3)
            try:
                reset_and_restore_database(db_name, logger=None)
            except Exception as e2:
                print(f"[Thread {idx}] Second attempt to reset database failed: {e2}")

    # Lock has been released, process the results
    print(f"[Thread {idx}] Released lock for database {db_name}, processing results...")

    # If successful, read the output, otherwise create a failure result
    if success and os.path.exists(tmp_output) and os.path.getsize(tmp_output) > 0:
        try:
            with open(tmp_output, "r") as f:
                evaluation_result = json.load(f)
                # Add instance_id to ensure correct sorting later
                evaluation_result["instance_id"] = instance_id
                # Clean up temporary files
                os.unlink(tmp_input)
                os.unlink(tmp_output)
                return evaluation_result
        except Exception as e:
            print(
                f"[Thread {idx}] Error reading output for instance {instance_id}: {e}"
            )

    # Clean up temporary files
    try:
        os.unlink(tmp_input)
        if os.path.exists(tmp_output):
            os.unlink(tmp_output)
    except Exception as e:
        print(f"[Thread {idx}] Error cleaning up temp files: {e}")

    # If any step failed, return a failure result
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


def process_queue(work_queue, results_dict, global_stats_lock, args, thread_idx):
    """Worker function to process items from the queue"""
    while True:
        try:
            # Get the next item from the queue with a timeout
            original_idx, instance_data = work_queue.get(timeout=1)
            instance_id = instance_data.get("instance_id", f"instance_{original_idx}")

            # Process the instance
            try:
                result = run_instance(instance_data, instance_id, args, thread_idx)
                with global_stats_lock:
                    results_dict[instance_id] = result
            except Exception as e:
                print(
                    f"[Thread {thread_idx}] Error processing instance {instance_id}: {e}"
                )
                with global_stats_lock:
                    results_dict[instance_id] = {
                        "instance_id": instance_id,
                        "status": "failed",
                        "error_message": f"Error in processing: {str(e)}",
                        "total_test_cases": len(instance_data.get("test_cases", [])),
                        "passed_test_cases": 0,
                        "failed_test_cases": [],
                        "solution_phase_execution_error": True,
                        "solution_phase_timeout_error": False,
                        "solution_phase_assertion_error": False,
                    }

            # Mark the task as done
            work_queue.task_done()

        except queue.Empty:
            # No more items in the queue
            break
        except Exception as e:
            print(f"[Thread {thread_idx}] Error in worker thread: {e}")
            # Try to mark task as done to avoid hanging
            try:
                work_queue.task_done()
            except:
                pass


def main():
    parser = argparse.ArgumentParser(
        description="Wrapper script to run MS SQL Server evaluation cases using multiple threads."
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

    # Set up logging
    base_output_folder = os.path.splitext(args.jsonl_file)[0]
    log_filename = f"{base_output_folder}_wrapper.log"
    logger = configure_logger(log_filename)
    logger.info(
        f"=== Starting MS SQL Server Evaluation via Wrapper Script (Multithreaded with DB locking) ==="
    )
    logger.info(
        f"Processing {len(data_list)} instances from {args.jsonl_file} using {args.num_threads} threads"
    )

    # Collect all database names
    all_db_names = set()
    for data in data_list:
        if "db_id" in data:
            all_db_names.add(data["db_id"])

    # Add signal handler for graceful termination
    def cleanup_handler(signum, frame):
        global cleanup_in_progress
        if cleanup_in_progress:
            logger.info("Cleanup already in progress, ignoring signal")
            return

        cleanup_in_progress = True
        logger.info(f"Received signal {signum}. Starting emergency cleanup...")
        try:
            comprehensive_database_cleanup(all_db_names, logger)
        except Exception as e:
            logger.error(f"Error during signal handler cleanup: {e}")
            try:
                emergency_cleanup(all_db_names, logger)
            except:
                pass
        finally:
            sys.exit(1)

    # Register signal handlers
    signal.signal(signal.SIGINT, cleanup_handler)
    signal.signal(signal.SIGTERM, cleanup_handler)

    # Ensure num_threads is at least 1 and not more than the number of instances
    num_threads = max(1, min(args.num_threads, len(data_list)))

    # Create a dictionary to store results, using instance_id as the key
    results_dict = {}
    global_stats_lock = threading.Lock()

    try:
        # Create a work queue
        work_queue = queue.Queue()
        for i, data in enumerate(data_list):
            work_queue.put((i, data))

        # Start worker threads
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(
                target=process_queue,
                args=(work_queue, results_dict, global_stats_lock, args, i),
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)

        # Show progress bar
        with tqdm(total=len(data_list), desc="Evaluating instances") as pbar:
            completed = 0
            while completed < len(data_list):
                with global_stats_lock:
                    current_completed = len(results_dict)

                if current_completed > completed:
                    pbar.update(current_completed - completed)
                    completed = current_completed

                time.sleep(0.1)

                # Check if all threads are still alive
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
        for data in data_list:
            instance_id = data.get("instance_id", f"instance_{data_list.index(data)}")
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

        # Sort results in original order
        results = []
        for data in data_list:
            instance_id = data.get("instance_id", f"instance_{data_list.index(data)}")
            if instance_id in results_dict:
                results.append(results_dict[instance_id])
            else:
                # This should not happen due to the check above, but adding fallback
                logger.error(
                    f"Missing result for instance {instance_id} (after final check)"
                )
                results.append(
                    {
                        "instance_id": instance_id,
                        "status": "failed",
                        "error_message": "Result missing from processing",
                        "solution_phase_execution_error": True,
                        "total_test_cases": len(data.get("test_cases", [])),
                        "passed_test_cases": 0,
                        "failed_test_cases": [],
                    }
                )

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
        total_passed_instances = sum(1 for r in results if r.get("status") == "success")

        # Generate report and output files
        generate_report_and_output(
            args.jsonl_file,
            data_list,
            [r.get("error_message", "") for r in results],
            results,
            number_of_execution_errors,
            number_of_timeouts,
            number_of_assertion_errors,
            total_passed_instances,
        )

        # Generate category report if requested
        base_output_folder = os.path.splitext(args.jsonl_file)[0]
        report_file_path = f"{base_output_folder}_report.txt"
        if args.report == "true":
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

        # Print summary to console
        print("\nEvaluation Summary:")
        print(f"Total instances: {len(results)}")
        print(f"Passed instances: {total_passed_instances}")
        print(f"Failed instances: {len(results) - total_passed_instances}")
        print(f"Execution errors: {number_of_execution_errors}")
        print(f"Timeouts: {number_of_timeouts}")
        print(f"Assertion errors: {number_of_assertion_errors}")

        if len(results) > 0:
            overall_accuracy = (total_passed_instances / len(results)) * 100
            print(f"Overall accuracy: {overall_accuracy:.2f}%")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        # Try to clean up in case of error
        try:
            comprehensive_database_cleanup(all_db_names, logger)
        except Exception as cleanup_error:
            logger.error(f"Cleanup after error failed: {cleanup_error}")
            try:
                emergency_cleanup(all_db_names, logger)
            except:
                pass

    finally:
        # Ensure final cleanup of databases
        global cleanup_in_progress
        if not cleanup_in_progress:
            cleanup_in_progress = True
            logger.info("Performing final cleanup of all databases")
            try:
                comprehensive_database_cleanup(all_db_names, logger)
            except Exception as e:
                logger.error(f"Final cleanup failed: {e}")
                try:
                    emergency_cleanup(all_db_names, logger)
                except:
                    pass

        logger.info(
            "=== MS SQL Server Evaluation via Wrapper Script (Multithreaded with DB locking) Completed ==="
        )


if __name__ == "__main__":
    main()
