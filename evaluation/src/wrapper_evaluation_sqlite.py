#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simplified wrapper script for SQLite evaluation
Fixed threading and queue management issues
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
from logger import configure_logger
from utils import load_jsonl, save_report_and_status
from sqlite_utils import create_ephemeral_db_copies, drop_ephemeral_dbs, cleanup_all_database_files


def run_single_instance(instance_data, instance_id, args, ephemeral_db_path, logger):
    """Run a single evaluation instance in a separate process"""

    # Create temporary input file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
        tmp_input = tmp.name
        json.dump(instance_data, tmp)

    # Create temporary output file
    tmp_output = tempfile.NamedTemporaryFile(suffix=".json", delete=False).name

    base_output_folder = os.path.splitext(args.jsonl_file)[0]
    log_file_path = f"{base_output_folder}_instance_{instance_id}.log"

    # Build command to run single instance evaluation script
    eval_script = os.path.join(
        os.path.dirname(__file__), "single_instance_eval_sqlite.py"
    )
    cmd = [
        "python3",
        eval_script,
        "--jsonl_file",
        tmp_input,
        "--output_file",
        tmp_output,
        "--mode",
        args.mode,  # Pass the mode from wrapper args
        "--logging",
        "false",
        "--log_file",
        log_file_path,
    ]

    # Set up environment with ephemeral database path
    env = os.environ.copy()
    env["EPHEMERAL_DB_PATH"] = ephemeral_db_path

    # Log the start
    logger.info(f"Starting instance {instance_id} with DB: {ephemeral_db_path}")

    try:
        # Run the subprocess with timeout
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=180,  # 3 minute timeout per instance
            env=env,
        )

        success = result.returncode == 0

        if not success:
            logger.error(f"Instance {instance_id} failed with code {result.returncode}")
            if result.stderr:
                logger.error(f"STDERR: {result.stderr[:300]}...")

    except subprocess.TimeoutExpired:
        logger.error(f"Instance {instance_id} timed out after 180 seconds")
        success = False
    except Exception as e:
        logger.error(f"Exception running instance {instance_id}: {e}")
        success = False

    # Process results
    if success and os.path.exists(tmp_output) and os.path.getsize(tmp_output) > 0:
        try:
            with open(tmp_output, "r") as f:
                evaluation_result = json.load(f)
                evaluation_result["instance_id"] = instance_id
                evaluation_result["_index"] = instance_data.get("_index")
                logger.info(f"Instance {instance_id} completed successfully")
                return evaluation_result
        except Exception as e:
            logger.error(f"Error reading output for instance {instance_id}: {e}")

    # Clean up temporary files
    try:
        os.unlink(tmp_input)
        if os.path.exists(tmp_output):
            os.unlink(tmp_output)
    except Exception as e:
        logger.error(f"Error cleaning up temp files for {instance_id}: {e}")

    # Return failure result
    logger.error(f"Instance {instance_id} failed completely")
    return {
        "instance_id": instance_id,
        "_index": instance_data.get("_index"),
        "status": "failed",
        "error_message": "Failed to evaluate instance (subprocess error)",
        "total_test_cases": len(instance_data.get("test_cases", [])),
        "passed_test_cases": 0,
        "failed_test_cases": [],
        "evaluation_phase_execution_error": True,
        "evaluation_phase_timeout_error": False,
        "evaluation_phase_assertion_error": False,
    }


def process_instances_batch(instances_batch, ephemeral_db_paths, args, logger):
    """Process a batch of instances using available ephemeral databases"""
    results = []

    for i, (instance_data, instance_id) in enumerate(instances_batch):
        # Use round-robin assignment of ephemeral databases
        db_name = instance_data.get(
            "selected_database", instance_data.get("db_id", "unknown")
        )
        if db_name in ephemeral_db_paths and ephemeral_db_paths[db_name]:
            ephemeral_db_path = ephemeral_db_paths[db_name][
                i % len(ephemeral_db_paths[db_name])
            ]
        else:
            logger.error(f"No ephemeral database available for {db_name}")
            results.append(
                {
                    "instance_id": instance_id,
                    "_index": instance_data.get("_index"),
                    "status": "failed",
                    "error_message": f"No ephemeral database available for {db_name}",
                    "total_test_cases": len(instance_data.get("test_cases", [])),
                    "passed_test_cases": 0,
                    "failed_test_cases": [],
                    "evaluation_phase_execution_error": True,
                    "evaluation_phase_timeout_error": False,
                    "evaluation_phase_assertion_error": False,
                }
            )
            continue

        # Run the instance
        result = run_single_instance(
            instance_data, instance_id, args, ephemeral_db_path, logger
        )
        results.append(result)

        # Small delay to avoid overwhelming the system
        # time.sleep(0.1)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Simplified wrapper script for SQLite evaluation"
    )
    parser.add_argument("--jsonl_file", required=True, help="Path to JSONL file")
    parser.add_argument(
        "--limit", type=int, default=5000, help="Limit number of instances"
    )
    parser.add_argument("--skip", type=int, default=0, help="Skip first N instances")
    parser.add_argument(
        "--num_threads", type=int, default=4, help="Number of parallel threads"
    )
    parser.add_argument("--logging", type=str, default="true", help="Enable logging")
    parser.add_argument("--mode", choices=["gold", "pred"], default="pred", help="Mode")
    parser.add_argument(
        "--batch_size", type=int, default=10, help="Batch size for processing"
    )
    parser.add_argument(
        "--db_dir",
        type=str,
        default="./sqlite_databases_test",
        help="Path to the directory containing SQLite databases",
    )

    args = parser.parse_args()

    # Load and process data
    print(f"Loading data from {args.jsonl_file}...")
    data_list = load_jsonl(args.jsonl_file)

    if not data_list:
        print("No data found in JSONL file.")
        sys.exit(1)

    # Add _index to preserve original order for multi-threaded processing
    for i, data in enumerate(data_list):
        data["_index"] = i

    original_count = len(data_list)
    print(f"Loaded {original_count} instances")

    # Apply skip and limit
    if args.skip > 0:
        data_list = data_list[args.skip :]
        print(f"Skipped first {args.skip} instances")

    if args.limit is not None:
        data_list = data_list[: args.limit]
        print(f"Limited to {args.limit} instances")

    final_count = len(data_list)
    print(f"Processing {final_count} instances")

    # Collect unique database names
    all_db_names = set()
    for d in data_list:
        if "selected_database" in d:
            all_db_names.add(d["selected_database"])
        else:
            all_db_names.add(d["db_id"])

    print(f"Found {len(all_db_names)} unique databases: {sorted(all_db_names)}")

    # Setup logging
    base_output_folder = os.path.splitext(args.jsonl_file)[0]
    log_filename = f"{base_output_folder}_simple_wrapper.log"
    logger = configure_logger(log_filename)

    logger.info("=== Starting Simple SQLite Evaluation Wrapper ===")
    logger.info(f"Processing {final_count} instances")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Databases: {sorted(all_db_names)}")

    # Create ephemeral database copies
    print("Creating ephemeral database copies...")
    try:
        ephemeral_db_pool_dict = create_ephemeral_db_copies(
            base_db_names=all_db_names,
            num_copies=args.num_threads,
            pg_password="123123",
            logger=logger,
            db_dir=args.db_dir,
        )
        print("✓ Ephemeral database copies created")
    except Exception as e:
        logger.error(f"Failed to create ephemeral database copies: {e}")
        print(f"✗ Error creating ephemeral databases: {e}")
        sys.exit(1)

    # Prepare instances for processing
    instances_with_ids = []
    for i, data in enumerate(data_list):
        instance_id = data.get("instance_id", f"instance_{i}")
        instances_with_ids.append((data, instance_id))

    # Split instances into batches
    batches = []
    for i in range(0, len(instances_with_ids), args.batch_size):
        batch = instances_with_ids[i : i + args.batch_size]
        batches.append(batch)

    print(f"Split {final_count} instances into {len(batches)} batches")

    # Process batches
    all_results = []

    with tqdm(total=final_count, desc="Processing instances", unit="instance") as pbar:

        if args.num_threads == 1:
            # Single-threaded processing
            for batch_idx, batch in enumerate(batches):
                logger.info(f"Processing batch {batch_idx + 1}/{len(batches)}")
                batch_results = process_instances_batch(
                    batch, ephemeral_db_pool_dict, args, logger
                )
                all_results.extend(batch_results)
                pbar.update(len(batch))

                # Force garbage collection
                gc.collect()

        else:
            # Multi-threaded processing
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=args.num_threads
            ) as executor:
                # Submit all batches
                future_to_batch = {}
                for batch_idx, batch in enumerate(batches):
                    future = executor.submit(
                        process_instances_batch,
                        batch,
                        ephemeral_db_pool_dict,
                        args,
                        logger,
                    )
                    future_to_batch[future] = batch_idx

                # Process completed batches
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch_idx = future_to_batch[future]
                    try:
                        batch_results = future.result()
                        all_results.extend(batch_results)
                        pbar.update(len(batch_results))
                        logger.info(f"Completed batch {batch_idx + 1}/{len(batches)}")
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_idx}: {e}")
                        # Add error results for this batch
                        batch = batches[batch_idx]
                        for data, instance_id in batch:
                            error_result = {
                                "instance_id": instance_id,
                                "_index": data.get("_index"),
                                "status": "failed",
                                "error_message": f"Batch processing error: {e}",
                                "total_test_cases": len(data.get("test_cases", [])),
                                "passed_test_cases": 0,
                                "failed_test_cases": [],
                                "evaluation_phase_execution_error": True,
                                "evaluation_phase_timeout_error": False,
                                "evaluation_phase_assertion_error": False,
                            }
                            all_results.append(error_result)
                        pbar.update(len(batch))

                    # Force garbage collection
                    gc.collect()

    # Sort results by _index to maintain original order (handles multi-threading)
    all_results.sort(key=lambda x: x.get("_index", 0))

    # Calculate statistics
    total_instances = len(all_results)
    passed_instances = sum(1 for r in all_results if r["status"] == "success")
    failed_instances = total_instances - passed_instances

    execution_errors = sum(
        1 for r in all_results if r.get("evaluation_phase_execution_error", False)
    )
    timeout_errors = sum(
        1 for r in all_results if r.get("evaluation_phase_timeout_error", False)
    )
    assertion_errors = sum(
        1 for r in all_results if r.get("evaluation_phase_assertion_error", False)
    )

    overall_accuracy = (
        (passed_instances / total_instances * 100) if total_instances > 0 else 0.0
    )

    # Print summary
    print(f"\n" + "=" * 60)
    print("EVALUATION SUMMARY")
    print("=" * 60)
    print(f"Total instances: {total_instances}")
    print(f"Passed instances: {passed_instances}")
    print(f"Failed instances: {failed_instances}")
    print(f"Execution errors: {execution_errors}")
    print(f"Timeout errors: {timeout_errors}")
    print(f"Assertion errors: {assertion_errors}")
    print(f"Overall accuracy: {overall_accuracy:.2f}%")
    print("=" * 60)

    # Save results
    timestamp = datetime.now().isoformat(sep=" ", timespec="microseconds")
    report_file_path = f"{base_output_folder}_simple_report.txt"

    try:
        save_report_and_status(
            report_file_path,
            all_results,
            data_list,
            execution_errors,
            timeout_errors,
            assertion_errors,
            overall_accuracy,
            timestamp,
            logger,
        )
        print(f"\nReport saved: {report_file_path}")
    except Exception as e:
        print(f"Error saving report: {e}")
        logger.error(f"Error saving report: {e}")

    # Save output with status
    output_jsonl_file = f"{base_output_folder}_simple_output_with_status.jsonl"
    try:
        with open(output_jsonl_file, "w") as f:
            for i, data in enumerate(data_list):
                if i < len(all_results):
                    data["status"] = all_results[i]["status"]
                    data["error_message"] = all_results[i].get("error_message")
                # Remove _index before writing to output
                if "_index" in data:
                    del data["_index"]
                f.write(json.dumps(data) + "\n")
        print(f"Output saved: {output_jsonl_file}")
    except Exception as e:
        print(f"Error saving output: {e}")
        logger.error(f"Error saving output: {e}")

    # Cleanup ephemeral databases
    print("\nCleaning up ephemeral databases...")
    try:
        drop_ephemeral_dbs(ephemeral_db_pool_dict, "123123", logger)
        print("✓ Ephemeral databases cleaned up")
    except Exception as e:
        print(f"✗ Ephemeral cleanup error: {e}")
        logger.error(f"Ephemeral cleanup error: {e}")

    # Comprehensive cleanup of all temporary files (WAL, SHM, etc.)
    print("\nPerforming comprehensive database cleanup...")
    try:
        files_deleted, size_freed = cleanup_all_database_files(args.db_dir, logger)
        size_mb = size_freed / (1024 * 1024)
        print(f"✓ Comprehensive cleanup completed: {files_deleted} files deleted, {size_mb:.2f} MB freed")
    except Exception as e:
        print(f"✗ Comprehensive cleanup error: {e}")
        logger.error(f"Comprehensive cleanup error: {e}")

    print(f"\nEvaluation completed! Check logs: {log_filename}")


if __name__ == "__main__":
    main()
