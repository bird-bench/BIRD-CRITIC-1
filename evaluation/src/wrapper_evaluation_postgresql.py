#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wrapper script to run PostgreSQL evaluation instances in parallel using multiple threads.
This prevents database connection issues or OOM issues from terminating the entire evaluation.
Includes synchronization mechanisms to prevent database template conflicts.
"""

import argparse
import json
import os
import sys
import subprocess
import tempfile
import time
import gc
import concurrent.futures
import threading
from datetime import datetime
from tqdm import tqdm
from postgresql_utils import (
    load_jsonl,
    save_report_and_status,
    generate_category_report,
)
from logger import configure_logger

# Create a dictionary to store database locks
db_template_locks = {}
# Create a lock to protect access to the db_template_locks dictionary
template_locks_lock = threading.Lock()


def get_db_lock(db_name):
    """Get a lock for the specified database, create one if it doesn't exist"""
    with template_locks_lock:
        # Extract base database name (remove possible _process_N suffix)
        base_db_name = db_name.split("_process_")[0]
        template_db_name = f"{base_db_name}_template"

        if template_db_name not in db_template_locks:
            db_template_locks[template_db_name] = threading.Lock()

        return db_template_locks[template_db_name]


def run_instance(instance_data, instance_id, args, idx):
    """Run a single evaluation instance in a separate process"""

    # Get the database name used by this instance
    db_name = instance_data.get("db_id", "")
    if not db_name:
        print(f"Warning: Instance {instance_id} has no db_id specified.")
        db_name = "unknown_db"

    # Create temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
        tmp_input = tmp.name
        json.dump(instance_data, tmp)

    # Create temporary output file
    tmp_output = tempfile.NamedTemporaryFile(suffix=".json", delete=False).name

    # Create log directory in the same location as the input file
    log_dir = os.path.dirname(os.path.abspath(args.jsonl_file))
    instance_log_file = os.path.join(log_dir, f"instance_{instance_id}.log")

    # Build command to run single instance evaluation script
    cmd = [
        "python",
        os.path.join(os.path.dirname(__file__), "single_instance_eval_postgresql.py"),
        "--jsonl_file",
        tmp_input,
        "--output_file",
        tmp_output,
        "--mode",
        args.mode,
        "--logging",
        args.logging,
        "--log_file",
        instance_log_file,  # Pass complete log file path
    ]

    # Get the corresponding database template lock
    db_lock = get_db_lock(db_name)

    print(f"[Thread {idx}] Running instance {instance_id} with database {db_name}...")

    # Use lock to ensure the same template is not used to create multiple databases simultaneously
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

        # Add a short delay to ensure database operations are completely finished
        time.sleep(1)

    # Lock has been released, process results
    print(f"[Thread {idx}] Released lock for database {db_name}, processing results...")

    # If successful, read output, otherwise create failure result
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
    except:
        pass

    # If any step fails, return failure result
    return {
        "instance_id": instance_id,
        "status": "failed",
        "error_message": "Failed to evaluate instance (process error)",
        "total_test_cases": len(instance_data.get("test_cases", [])),
        "passed_test_cases": 0,
        "failed_test_cases": [],
        "evaluation_phase_execution_error": True,
        "evaluation_phase_timeout_error": False,
        "evaluation_phase_assertion_error": False,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Wrapper script to run PostgreSQL evaluation cases using multiple threads."
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
        default="gold",
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
        f"=== Starting PostgreSQL Evaluation via Wrapper Script (Multithreaded with DB locking) ==="
    )
    logger.info(
        f"Processing {len(data_list)} instances from {args.jsonl_file} using {args.num_threads} threads"
    )

    # Ensure num_threads is at least 1
    num_threads = max(1, min(args.num_threads, len(data_list)))

    # Preprocess and group instances by database
    # This allows arranging instances that use the same database in different batches
    db_groups = {}
    for i, data in enumerate(data_list):
        db_name = data.get("db_id", "unknown")
        if db_name not in db_groups:
            db_groups[db_name] = []
        db_groups[db_name].append((i, data))

    # Create cyclically assigned list
    ordered_instances = []
    while any(len(group) > 0 for group in db_groups.values()):
        for db_name in list(db_groups.keys()):
            if db_groups[db_name]:
                ordered_instances.append(db_groups[db_name].pop(0))

    # Create dictionary to store results, using instance_id as key to ensure correct sorting
    results_dict = {}

    # Process instances in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks
        future_to_instance = {
            executor.submit(
                run_instance,
                data,
                data.get("instance_id", f"instance_{original_idx}"),
                args,
                thread_idx,  # Pass thread index for logging
            ): (original_idx, data.get("instance_id", f"instance_{original_idx}"))
            for thread_idx, (original_idx, data) in enumerate(ordered_instances)
        }

        # Process completed results
        for future in tqdm(
            concurrent.futures.as_completed(future_to_instance),
            desc="Evaluating instances",
            total=len(data_list),
        ):
            original_idx, instance_id = future_to_instance[future]
            try:
                result = future.result()
                # Store result, using original index to ensure correct sorting
                results_dict[instance_id] = result
            except Exception as e:
                logger.error(f"Error processing instance {instance_id}: {e}")
                # Add failure result
                error_result = {
                    "instance_id": instance_id,
                    "status": "failed",
                    "error_message": f"Error in wrapper: {str(e)}",
                    "total_test_cases": len(
                        data_list[original_idx].get("test_cases", [])
                    ),
                    "passed_test_cases": 0,
                    "failed_test_cases": [],
                    "evaluation_phase_execution_error": True,
                    "evaluation_phase_timeout_error": False,
                    "evaluation_phase_assertion_error": False,
                }
                results_dict[instance_id] = error_result

            # Force garbage collection after each instance completes
            gc.collect()

    # Sort results according to original order
    results = []
    for data in data_list:
        instance_id = data.get("instance_id", f"instance_{data_list.index(data)}")
        if instance_id in results_dict:
            results.append(results_dict[instance_id])
        else:
            # This should not happen, but add a fallback
            logger.error(f"Missing result for instance {instance_id}")
            results.append(
                {
                    "instance_id": instance_id,
                    "status": "failed",
                    "error_message": "Result missing from processing",
                    "evaluation_phase_execution_error": True,
                    "total_test_cases": len(data.get("test_cases", [])),
                    "passed_test_cases": 0,
                    "failed_test_cases": [],
                }
            )

    # Compile statistics
    number_of_execution_errors = sum(
        1 for r in results if r.get("evaluation_phase_execution_error", False)
    )
    number_of_timeouts = sum(
        1 for r in results if r.get("evaluation_phase_timeout_error", False)
    )
    number_of_assertion_errors = sum(
        1 for r in results if r.get("evaluation_phase_assertion_error", False)
    )
    total_passed_instances = sum(1 for r in results if r.get("status") == "success")

    # Generate summary report
    total_instances = len(results)
    total_errors = (
        number_of_execution_errors + number_of_timeouts + number_of_assertion_errors
    )
    overall_accuracy = (
        ((total_instances - total_errors) / total_instances * 100)
        if total_instances > 0
        else 0.0
    )
    timestamp = datetime.now().isoformat(sep=" ", timespec="microseconds")

    # Save report and generate output file
    report_file_path = f"{base_output_folder}_report.txt"
    save_report_and_status(
        report_file_path,
        results,
        data_list,
        number_of_execution_errors,
        number_of_timeouts,
        number_of_assertion_errors,
        overall_accuracy,
        timestamp,
        logger,
    )

    print("Overall report generated:", report_file_path)

    # Output JSONL with status
    output_jsonl_file = f"{base_output_folder}_output_with_status.jsonl"
    with open(output_jsonl_file, "w") as f:
        for data in data_list:
            instance_id = data.get("instance_id")
            for result in results:
                if data.get("instance_id") == result.get("instance_id"):
                    data["status"] = result["status"]
                    data["error_message"] = result.get("error_message")
                    # Remove potentially large fields
                    data.pop("prompt", None)
                    # data.pop("response", None)
                    data.pop("reasoning_content", None)
                    f.write(json.dumps(data, ensure_ascii=False) + "\n")
                    break

    # Generate difficulty level performance report if requested
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
    print(f"Total instances: {total_instances}")
    print(f"Passed instances: {total_passed_instances}")
    print(f"Failed instances: {total_instances - total_passed_instances}")
    print(f"Execution errors: {number_of_execution_errors}")
    print(f"Timeouts: {number_of_timeouts}")
    print(f"Assertion errors: {number_of_assertion_errors}")
    print(f"Overall accuracy: {overall_accuracy:.2f}%")

    logger.info(
        "=== PostgreSQL Evaluation via Wrapper Script (Multithreaded with DB locking) Completed ==="
    )


if __name__ == "__main__":
    main()
