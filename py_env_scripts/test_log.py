#!/usr/bin/env python3

import argparse
import json
import logging
import re
import sys
import os
from mysql_setup import perform_query_on_mysql_databases
from postgresql_setup import perform_query_on_postgresql_databases


def load_json(file_path):
    """Load the JSON data from a file."""
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def select_query_function(language):
    """Select the database query function based on language."""
    if language.lower() == "mysql":
        return perform_query_on_mysql_databases
    elif language.lower() == "postgresql":
        return perform_query_on_postgresql_databases
    else:
        raise ValueError(f"Unsupported database language: {language}")


def extract_error_sql(query_text):
    """Extract SQL from a text field using regex to simulate the error SQL."""
    sql_pattern = r"```sql\s*(.*?)\s*```"
    matches = re.findall(sql_pattern, query_text, re.DOTALL)
    if matches:
        return matches[0].strip()
    else:
        logging.warning("No error SQL found in the query text.")
        return None


def log_section_header(section_title):
    """Log a formatted section header with separators."""
    separator = f"{'=' * 20} {section_title} {'=' * 20}"
    logging.info(f"\n\n{separator}\n")


def log_section_footer():
    """Log a formatted section footer with separators."""
    separator = f"{'=' * 60}"
    logging.info(f"\n\n{separator}\n")


def execute_queries(queries, db_name, perform_query_func, section_title):
    """Execute a list of SQL queries and log the results, with section headers."""
    log_section_header(section_title)
    for i, query in enumerate(queries):
        try:
            logging.info(f"Executing query {i + 1}/{len(queries)}: {query}")
            result = perform_query_func(query, db_name)
            logging.info(f"Query result: {result}")
        except Exception as e:
            logging.error(f"Error executing query {i + 1}: {e}")
            sys.exit(1)
    log_section_footer()


def execute_error_sql(error_sql, db_name, perform_query_func):
    """Execute the extracted error SQL and log any errors."""
    log_section_header("Error Reproduction")
    if error_sql:
        try:
            logging.info(f"Executing error SQL:\n{error_sql}")
            perform_query_func(error_sql, db_name)
            logging.info("Error SQL executed without error (unexpected).")
        except Exception as e:
            logging.info(f"Expected error encountered: {e}")
    else:
        logging.warning("No error SQL provided for reproduction.")
    log_section_footer()


def execute_test_cases(test_cases, db_name, perform_query_func):
    """Execute the test cases and log detailed information about each one."""
    log_section_header("Evaluation")
    for i, test_case in enumerate(test_cases):
        logging.info(f"Starting test case {i + 1}/{len(test_cases)}")
        logging.info(f"Test case content:\n{test_case}")

        try:
            exec(test_case, globals(), locals())
            logging.info(f"Test case {i + 1} passed.")
        except AssertionError as e:
            logging.error(f"Test case {i + 1} failed due to assertion error: {e}")
        except Exception as e:
            logging.error(f"Test case {i + 1} failed due to error: {e}")
            sys.exit(1)
    log_section_footer()


def main():
    parser = argparse.ArgumentParser(
        description="Execute SQL solution and test cases from JSON file."
    )
    parser.add_argument(
        "json_file", help="Path to the JSON file containing the dataset instance."
    )
    args = parser.parse_args()

    # Load JSON data
    data = load_json(args.json_file)

    # Create log filename based on JSON filename
    log_filename = os.path.splitext(args.json_file)[0] + ".log"

    # Initialize logging
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info("Starting execution")

    # Select query function based on language
    perform_query_func = select_query_function(data["language"])
    db_name = data["db_id"]

    # Step 1: Execute preprocess SQL
    execute_queries(
        data["preprocess_sql"], db_name, perform_query_func, "Preprocess SQL"
    )

    # Step 2: Extract and execute the error SQL
    error_sql = extract_error_sql(data["query"])
    execute_error_sql(error_sql, db_name, perform_query_func)

    # Step 3: Execute solution SQL
    execute_queries(data["solution_sql"], db_name, perform_query_func, "Solution SQL")

    # Step 4: Execute test cases with detailed logging for each case
    execute_test_cases(data["test_cases"], db_name, perform_query_func)

    # Step 5: Execute clean up SQLs
    execute_queries(data["clean_up_sql"], db_name, perform_query_func, "Clean Up SQL")

    logging.info("Execution complete.")


if __name__ == "__main__":
    main()
