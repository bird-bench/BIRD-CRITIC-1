#!/usr/bin/env python3

import argparse
import json
import logging
import re
import sys
import os
from mysql_setup import perform_query_on_mysql_databases
import pymysql
import sqlparse
import tqdm
import subprocess


# Define database-to-table mapping
DATABASE_MAPPING = {
    "debit_card_specializing": [
        "customers",
        "gasstations",
        "products",
        "yearmonth",
        "transactions_1k",
    ],
    "financial": [
        "loan",
        "client",
        "district",
        "trans",
        "account",
        "card",
        "order",
        "disp",
    ],
    "formula_1": [
        "circuits",
        "status",
        "drivers",
        "driverStandings",
        "races",
        "constructors",
        "constructorResults",
        "lapTimes",
        "qualifying",
        "pitStops",
        "seasons",
        "constructorStandings",
        "results",
    ],
    "california_schools": ["schools", "satscores", "frpm"],
    "card_games": [
        "legalities",
        "cards",
        "rulings",
        "set_translations",
        "sets",
        "foreign_data",
    ],
    "european_football_2": [
        "Team_Attributes",
        "Player",
        "Match",
        "League",
        "Country",
        "Player_Attributes",
        "Team",
    ],
    "thrombosis_prediction": ["Laboratory", "Patient", "Examination"],
    "toxicology": ["bond", "molecule", "atom", "connected"],
    "student_club": [
        "income",
        "budget",
        "zip_code",
        "expense",
        "member",
        "attendance",
        "event",
        "major",
    ],
    "superhero": [
        "gender",
        "superpower",
        "publisher",
        "superhero",
        "colour",
        "attribute",
        "hero_power",
        "race",
        "alignment",
        "hero_attribute",
    ],
    "codebase_community": [
        "postLinks",
        "postHistory",
        "badges",
        "posts",
        "users",
        "tags",
        "votes",
        "comments",
    ],
}


def load_jsonl(file_path):
    with open(file_path, "r") as file:
        return [json.loads(line) for line in file]


def select_query_function(language):
    """Select the database query function based on language."""
    if language.lower() == "mysql":
        return perform_query_on_mysql_databases
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


def log_section_header(section_title, logger):
    """Log a formatted section header with separators."""
    separator = f"{'=' * 20} {section_title} {'=' * 20}"
    logger.info(f"\n\n{separator}\n")


def log_section_footer(logger):
    """Log a formatted section footer with separators."""
    separator = f"{'=' * 60}"
    logger.info(f"\n\n{separator}\n")


def configure_logger(log_filename):
    """Create and configure a new logger instance."""
    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.INFO)

    # Remove existing handlers (if any)
    if logger.handlers:
        logger.handlers.clear()

    # Create file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)

    return logger


def reset_and_restore_database(db_name, mysql_password, logger):
    """Delete, recreate the database, and restore tables using the mysql command."""
    try:
        mysql_host = "bird_critic_mysql"  # Hostname for Docker network
        mysql_user = "root"
        mysql_port = 3306

        # Drop and recreate the database using mysql command
        drop_create_db_command = [
            "mysql",
            f"-h{mysql_host}",
            f"-P{mysql_port}",
            f"-u{mysql_user}",
            f"-p{mysql_password}",
            "-e",
            f"DROP DATABASE IF EXISTS `{db_name}`; CREATE DATABASE `{db_name}`;",
        ]
        subprocess.run(drop_create_db_command, check=True)
        logger.info(f"Database {db_name} dropped and recreated.")

        # Restore tables from the SQL dump files
        table_names = DATABASE_MAPPING.get(db_name.lower(), [])
        for table in table_names:
            dump_file_path = f"./mysql_table_dumps/{table}.sql"
            if os.path.isfile(dump_file_path):
                logger.info(f"Importing table {table} into database {db_name}.")
                with open(dump_file_path, "r") as sql_file:
                    import_command = [
                        "mysql",
                        f"-h{mysql_host}",
                        f"-P{mysql_port}",
                        f"-u{mysql_user}",
                        f"-p{mysql_password}",
                        db_name,
                    ]
                    subprocess.run(import_command, stdin=sql_file, check=True)
                logger.info(f"Table {table} successfully imported into {db_name}.")
            else:
                logger.warning(f"SQL dump file for table {table} not found. Skipping.")

    except subprocess.CalledProcessError as e:
        logger.error(f"Error resetting and restoring the database: {e}")
        sys.exit(1)


def execute_queries(queries, db_name, perform_query_func, section_title, logger):
    """Execute a list of SQL queries and log the results, with section headers."""
    log_section_header(section_title, logger)
    # We only care about the last query result
    query_result = None
    for i, query in enumerate(queries):
        try:
            logger.info(f"Executing query {i + 1}/{len(queries)}: {query}")
            result = perform_query_func(query, db_name)
            query_result = result
            logger.info(f"Query result: {result}")
        except Exception as e:
            logger.error(f"Error executing query {i + 1}: {e}")
            continue
    log_section_footer(logger)
    return query_result


def execute_error_sql(error_sql, db_name, perform_query_func, logger):
    """Execute the extracted error SQL and log any errors."""
    log_section_header("Error Reproduction", logger)
    error_message = None
    error_sql_result = None
    if error_sql:
        try:
            logger.info(f"Executing error SQL:\n{error_sql}")
            error_sql_result = perform_query_func(error_sql, db_name)
            logger.info(f"Error SQL executed without error (unexpected).")
        except Exception as e:
            logger.info(f"Expected error encountered: {e}")
            error_message = str(e)
    else:
        logger.warning("No error SQL provided for reproduction.")
    log_section_footer(logger)
    return error_message, error_sql_result


def execute_test_cases(test_cases, sql_result, logger):
    """Execute the test cases and log detailed information about each one."""
    logger.info(f"Starting Evaluation")
    for i, test_case in enumerate(test_cases):
        logger.info(f"Starting test case {i + 1}/{len(test_cases)}")
        logger.info(f"Test case content:\n{test_case}")

        # Prepare environment for the test case
        local_env = {"sol_sql_result": sql_result}
        # add the import statement to the test case
        test_case = "from datetime import date\n" + test_case
        try:
            exec(test_case, globals(), local_env)
            logger.info(f"Test case {i + 1} passed.")
        except AssertionError as e:
            logger.error(f"Test case {i + 1} failed due to assertion error: {e}")
        except Exception as e:
            logger.error(f"Test case {i + 1} failed due to error: {e}")
            continue
    logger.info("Evaluation complete.")


def main():
    parser = argparse.ArgumentParser(
        description="Execute SQL solution and test cases from JSON file."
    )
    parser.add_argument(
        "--json_file", help="Path to the JSON file containing the dataset instance."
    )
    parser.add_argument(
        "--mysql_password",
        help="MySQL root password for resetting the database.",
        default="123123",
    )
    args = parser.parse_args()

    # Load JSONL data
    data_list = load_jsonl(args.json_file)
    try:
        # Iterate over each question in the JSONL file
        for i, data in tqdm.tqdm(enumerate(data_list), desc="Evaluating questions..."):
            log_filename = (
                os.path.splitext(args.json_file)[0] + f"_question_{i + 1}.log"
            )

            # Configure logger for this question
            logger = configure_logger(log_filename)
            logger.info(f"Starting execution for question {i + 1}")

            try:
                db_name = data["db_id"]

                # Reset the database and restore tables
                logger.info(f"Resetting database {db_name} and restoring tables.")
                reset_and_restore_database(db_name, args.mysql_password, logger)
                logger.info("Database reset and tables restored.")

                # Select query function based on language
                perform_query_func = select_query_function(data["language"])

                # Step 1: Execute preprocess SQL
                execute_queries(
                    data["preprocess_sql"],
                    db_name,
                    perform_query_func,
                    "Preprocess SQL",
                    logger,
                )

                # Step 2: Extract and execute the error SQL
                # error_sql = extract_error_sql(data["query"])
                error_sql = data["error_sql"]
                error_message, error_sql_result = execute_error_sql(
                    error_sql, db_name, perform_query_func, logger
                )

                if error_message is None:
                    # No error when executing error SQL
                    # Run test cases with error_sql_result
                    logger.info(
                        "No error encountered during error SQL execution. Running test cases with error_sql_result."
                    )
                    execute_test_cases(data["test_cases"], error_sql_result, logger)
                else:
                    logger.info(
                        "Error encountered as expected during error SQL execution."
                    )

                # Step 3: Execute solution SQL
                sol_sql_result = execute_queries(
                    # [data["response_sol"]],
                    data["sol_sql"],
                    db_name,
                    perform_query_func,
                    "GPT Generated SQL",
                    logger,
                )

                # Step 4: Execute test cases with detailed logging for each case
                execute_test_cases(data["test_cases"], sol_sql_result, logger)

                # Step 5: Execute clean up SQLs
                execute_queries(
                    data["clean_up_sql"],
                    db_name,
                    perform_query_func,
                    "Clean Up SQL",
                    logger,
                )

                logger.info(f"Execution for question {i + 1} complete.\n")

            except Exception as e:
                logger.error(f"Error during execution for question {i + 1}: {e}")
                logger.error("Skipping to the next question.\n")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
