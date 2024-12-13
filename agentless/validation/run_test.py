import sys

parent_dir = "/app/agentless"
sys.path.append(parent_dir)
import os
import io
import json
from contextlib import redirect_stdout
import re
import argparse
from tqdm import tqdm
import concurrent.futures
from util.utils import (
    load_jsonl,
    setup_logger,
    cleanup_logger,
    remove_comments_and_docstrings,
)
from util.mysql_setup import perform_query_on_mysql_databases

# from sqlfluff import parse_one
# from sqlfluff.core import FluffConfig
from sqlglot import parse_one
from collections import Counter


def extract_sql_queries(response):
    """
    Extracts SQL queries from the given response string, handling both ```sql``` and ```SQL``` wrappers.

    Args:
        response (str): The response string containing SQL queries wrapped in ```sql ... ``` or ```SQL ... ```.

    Returns:
        list: A list of SQL queries extracted from the response.
    """
    # Define regex to extract SQL blocks, case-insensitive for `sql` or `SQL`
    sql_pattern = r"```(?:sql|SQL)(.*?)```"
    # Find all matches, strip unnecessary whitespaces, and return as a list
    # sql_queries = [
    #     match.strip() for match in re.findall(sql_pattern, response, re.DOTALL)
    # ]
    # return sql_queries
    sql_pattern = re.compile(sql_pattern, re.DOTALL)
    match = sql_pattern.search(response)

    if match:
        return match.group(1).strip()

    return None


def run_test(data, args):
    instance_id = data["instance_id"]
    log_file = os.path.join(
        args.output_folder, "select_final_logs", f"{instance_id}.log"
    )
    logger = setup_logger(log_file)

    logger.info(f"================ running test for {instance_id} ================")

    pred_sol_sqls = data["pred_sols"]
    normalized_tests = data["filtered_test_cases"]

    # We will run each predicted solution against all normalized tests.
    # Only keep the predicted solutions that produce "Issue resolved" in all tests.

    good_solutions = []

    for pred_sol in pred_sol_sqls:
        pred_sol_sql = extract_sql_queries(pred_sol["response"])
        # We'll assume there's a known test function like 'test_issue' that runs the sol_sql against data['db']
        all_passed = True
        for test_code in normalized_tests:
            buf = io.StringIO()
            with redirect_stdout(buf):
                # Construct the command that runs the test. We assume the test code
                # defines a test function like `test_issue(sql, db)` internally,
                # or sets up the environment so that this call is valid.
                pred_sol_sql = str(parse_one(pred_sol_sql, dialect=args.dialect))
                test_command = f"test_func('{pred_sol_sql}', '{data['db_id']}')"
                logger.info(f"Running test command:\n{test_command}")
                logger.info(f"Running test with:\n{test_code}\n{test_command}")
                try:
                    exec(test_code + "\n" + test_command, globals(), locals())
                except Exception as e:
                    # If something unexpected happens, treat as other issues
                    all_passed = False
                    logger.warning(f"Unexpected error during test execution: {e}")
                    break

            output = buf.getvalue()
            logger.info(f"Test output:\n{output}")
            if "Issue resolved" not in output:
                # If any test does not output "Issue resolved", this solution fails
                all_passed = False
                break

        if all_passed:
            # This solution passed all tests with "Issue resolved"
            good_solutions.append(pred_sol_sql)

    data.pop("pred_sqls", None)
    # Filter out solutions that didn't produce "Issue resolved"
    data["final_pred_sols"] = good_solutions

    if not good_solutions:
        logger.info("No solutions resolved the issue.")
    else:
        logger.info(f"{len(good_solutions)} solutions resolved the issue.")
    with open(args.output_file, "a") as f:
        f.write(json.dumps(data) + "\n")
    cleanup_logger(logger)


def run_reproduction_tests(args):
    with open(f"{args.output_folder}/args.json", "w") as f:
        json.dump(vars(args), f, indent=4)

    data_list = load_jsonl(args.processed_data)

    if args.num_threads == 1:
        for data in tqdm(data_list, total=len(data_list), colour="MAGENTA"):
            run_test(data, args)
    else:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.num_threads
        ) as executor:
            futures = {
                executor.submit(run_test, data, args): data for data in data_list
            }
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(data_list),
                colour="MAGENTA",
            ):
                future.result()


def rerank_sol(data, args):
    instance_id = data["instance_id"]
    log_file = os.path.join(
        args.output_folder, "select_final_logs", f"{instance_id}.log"
    )
    logger = setup_logger(log_file)

    logger.info(
        f"================ re-ranking solutions for {instance_id} ================"
    )
    pred_sol_sqls = data["final_pred_sols"]
    count = Counter()

    # We want to normalize each predicted solution to ignore surface-level differences.
    # We'll parse each solution into a syntax tree and then extract a canonical representation.
    # By doing so, we eliminate differences like extra spaces, newlines, and comments.

    normalized_forms = {}
    for sol_sql in pred_sol_sqls:
        try:
            # Parse the SQL into a syntax tree
            parsed = parse_one(sol_sql, dialect=args.dialect)
            if parsed is None:
                # If parsing failed, skip this solution
                logger.warning(f"Parsing returned None for SQL: {sol_sql}")
                continue

            # Convert the parsed tree back to a canonical representation
            # parsed.raw returns a standardized, re-constructed version of the SQL
            # that removes comments and normalizes whitespace.
            canonical_sql = str(parsed)

            normalized_forms[sol_sql] = canonical_sql
            count[canonical_sql] += 1

        except Exception as e:
            # If parsing or normalization fails, skip this solution
            logger.warning(f"Failed to parse or normalize SQL: {sol_sql}\nError: {e}")
            continue

    # Select the canonical form that appears the most frequently
    if len(count) == 0:
        logger.info("No valid parsed solutions found.")
        data["final_reranked_sql"] = []
    else:

        most_common_canonical, _ = count.most_common(1)[0]

        # Keep only the solutions that match the most common normalized form
        final_solutions = [
            sql
            for sql, canon in normalized_forms.items()
            if canon == most_common_canonical
        ]

        data["final_reranked_sql"] = final_solutions
        logger.info(
            f"Selected {len(final_solutions)} final solutions after re-ranking."
        )
    reranked_file = os.path.join(args.output_folder, "reranked.jsonl")
    with open(reranked_file, "a") as f:
        f.write(json.dumps(data) + "\n")


def rerank_sols(args):
    data_list = load_jsonl(args.output_file)

    if args.num_threads == 1:
        for data in tqdm(data_list, total=len(data_list), colour="MAGENTA"):
            rerank_sol(data, args)
    else:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.num_threads
        ) as executor:
            futures = {
                executor.submit(rerank_sol, data, args): data for data in data_list
            }
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(data_list),
                colour="MAGENTA",
            ):
                future.result()


def finalized_solutions(args):
    reranked_file = os.path.join(args.output_folder, "reranked.jsonl")
    data_list = load_jsonl(reranked_file)

    # reorder the data instance based on the instance_id
    data_list = sorted(data_list, key=lambda x: x["instance_id"])

    # rename the final_reranked_sql to pred_sql
    data_list = [
        {**data, "pred_sql": data.pop("final_reranked_sql")} for data in data_list
    ]

    # write the finalized solutions to the output file
    final_output_file = os.path.join(args.output_folder, "final_output.jsonl")
    with open(final_output_file, "w") as f:
        for data in data_list:
            f.write(json.dumps(data) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed_data", type=str, required=True)
    parser.add_argument("--output_folder", type=str, required=True)
    parser.add_argument("--dialect", type=str, required=True)
    parser.add_argument(
        "--num_threads",
        type=int,
        default=1,
        help="Number of threads to use for creating API requests",
    )

    args = parser.parse_args()

    if not os.path.exists(args.output_folder):
        os.makedirs(args.output_folder)
    if not os.path.exists(os.path.join(args.output_folder, "select_final_logs")):
        os.makedirs(os.path.join(args.output_folder, "select_final_logs"))

    args.output_file = os.path.join(args.output_folder, "output.jsonl")
    run_reproduction_tests(args)
    rerank_sols(args)
    finalized_solutions(args)


if __name__ == "__main__":
    main()
