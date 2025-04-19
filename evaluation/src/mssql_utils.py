#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database utility functions for SQL Server operations.
"""

import os, csv
import pymssql
from logger import PrintLogger, log_section_header, log_section_footer
import json
import sys
import re
import os

from datetime import datetime


#!/usr/bin/env python3
# -*- coding: utf-8 -*-


BCP_DATABASE_MAPPING = {
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
}

TABLE_ORDER = [table for tables in BCP_DATABASE_MAPPING.values() for table in tables]


SQLSERVER_COMMIT_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "create",
    "drop",
    "alter",
    "truncate",
    "merge",
    "grant",
    "revoke",
    "backup",
    "restore",
    "begin tran",
    "commit tran",
    "rollback tran",
    "save tran",
    "dbcc",
    "sp_",
)

DEFAULT_SQLSERVER_CONFIG = {
    "SERVER": "bird_critic_sqlserver",  # Docker service or container name
    "PORT": 1433,
    "USER": "sa",
    "PASSWORD": "Y.sa123123",
}


def perform_query_on_sqlserver_databases(query, db_name, conn=None, as_dict=False):
    if conn == None:
        conn = pymssql.connect(
            server=DEFAULT_SQLSERVER_CONFIG["SERVER"],
            port=DEFAULT_SQLSERVER_CONFIG["PORT"],
            user=DEFAULT_SQLSERVER_CONFIG["USER"],
            password=DEFAULT_SQLSERVER_CONFIG["PASSWORD"],
            database=db_name,
        )
    cursor = conn.cursor(as_dict=as_dict)
    try:
        cursor.execute(query)
        sql_tokens = query.lower().split()
        # print(sql_tokens)
        if any(kw in sql_tokens for kw in SQLSERVER_COMMIT_KEYWORDS):
            conn.commit()
        try:
            rows = cursor.fetchall()
        except pymssql.OperationalError:
            rows = None
        return rows, conn
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def close_sqlserver_connection(conn):
    if conn:
        conn.close()


def reset_and_restore_database(db_name, logger=None):
    """
    Reset and restore a SQL Server database to a known initial state.
    """
    if logger is None:
        logger = PrintLogger()

    logger.info(f"Resetting database [{db_name}] ...")

    master_conn = None
    try:
        master_conn = pymssql.connect(
            server="bird_critic_sqlserver",
            port=1433,
            user="sa",
            password="Y.sa123123",
            database="master",
        )
        master_conn.autocommit(True)
        cur = master_conn.cursor()

        kill_sql = f"""
        DECLARE @kill varchar(8000) = '';
        SELECT @kill = @kill + 'KILL ' + CONVERT(varchar(5), session_id) + ';'
        FROM sys.dm_exec_sessions
        WHERE 
            database_id = DB_ID('{db_name}')
            AND session_id <> @@SPID
            AND session_id > 50
            AND is_user_process = 1;

        EXEC(@kill);
        """
        logger.info(f"[Reset] Killing active connections for DB {db_name} ...")
        cur.execute(kill_sql)

        drop_sql = f"IF DB_ID('{db_name}') IS NOT NULL DROP DATABASE [{db_name}];"
        logger.info(f"[Reset] Dropping DB {db_name} if exists ...")
        cur.execute(drop_sql)

        backup_file = f"/app/mssql_table_dumps/{db_name}_template.bak"
        if os.path.exists(backup_file):
            logger.info(f"[Reset] Restoring DB {db_name} from {backup_file} ...")
            restore_sql = f"""
            RESTORE DATABASE [{db_name}]
            FROM DISK = '{backup_file}'
            WITH REPLACE,
                 RECOVERY,
                 STATS = 5
            """
            cur.execute(restore_sql)
            logger.info(f"[Reset] Database {db_name} restored successfully.")
        else:
            logger.warning(
                f"[Reset] {backup_file} not found, creating empty DB {db_name}"
            )
            create_db_sql = f"CREATE DATABASE [{db_name}];"
            cur.execute(create_db_sql)
            logger.info(f"[Reset] Empty database {db_name} created.")

    except Exception as e:
        logger.error(f"Error resetting {db_name} from backup or template: {e}")
        raise
    finally:
        if master_conn:
            master_conn.close()


def get_connection_for_phase(db_name, logger=None):
    """
    Obtain a dedicated connection for the current phase.
    """
    if logger is None:
        logger = PrintLogger()
    logger.info(f"Acquiring dedicated connection for phase on db: {db_name}")
    rows, conn = perform_query_on_sqlserver_databases("SELECT 1;", db_name, None, False)
    return conn


def execute_queries(
    queries,
    db_name,
    conn=None,
    logger=None,
    section_title="",
    is_solution=True,
    as_dict=False,
):
    """
    Execute the given list of SQL queries in sequence.
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
            query_result, conn = perform_query_on_sqlserver_databases(
                query, db_name, conn, as_dict=as_dict
            )
            logger.info(f"[execute_queries] Query result:: {query_result}")
        except pymssql.OperationalError as e:
            logger.error(f"[execute_queries] OperationalError executing query {i}: {e}")
            if is_solution:
                execution_error = True
        except pymssql.Error as e:
            logger.error(f"[execute_queries] pymssql Error executing query {i}: {e}")
            if is_solution:
                execution_error = True
        except Exception as e:
            logger.error(f"[execute_queries] Generic error executing query {i}: {e}")
            if is_solution:
                execution_error = True
        finally:
            logger.info(f"[{section_title}] DB: {db_name}, conn: {conn}")

    log_section_footer(logger)
    return query_result, execution_error, timeout_error


def run_preprocessing(preprocess_sql, db_name, logger, conn):
    """
    If there is preprocessing SQL, execute it in sequence.
    """
    if preprocess_sql:
        execute_queries(preprocess_sql, db_name, conn, logger, "Preprocess SQL", False)


def load_jsonl(file_path):
    """
    Read data from a JSONL file and return a list where each element is a JSON record.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return [json.loads(line) for line in file]
    except Exception as e:
        print(f"Failed to load JSONL file: {e}")
        sys.exit(1)


def split_field(data, field_name):
    """
    Split the SQL statements in data[field_name] into a list.
    """
    field_value = data.get(field_name, "")
    if not field_value:
        return []
    if isinstance(field_value, str):
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


def generate_report_and_output(
    jsonl_file,
    data_list,
    error_messages,
    question_test_case_results,
    number_of_execution_errors,
    number_of_timeouts,
    number_of_assertion_errors,
    total_passed_instances,
):
    """
    Generate the final report and output JSONL with status.
    """
    total_instances = len(data_list)
    total_errors = (
        number_of_execution_errors + number_of_timeouts + number_of_assertion_errors
    )
    total_passed_instances_wo_error_pass = total_passed_instances
    overall_accuracy = (
        (total_passed_instances_wo_error_pass / total_instances * 100)
        if total_instances > 0
        else 0.0
    )
    timestamp = datetime.now().isoformat(sep=" ", timespec="microseconds")
    base_output_folder = os.path.splitext(jsonl_file)[0]
    report_file_path = f"{base_output_folder}_report.txt"
    output_data = data_list.copy()

    try:
        with open(report_file_path, "w", encoding="utf-8") as report_file:
            report_file.write("--------------------------------------------------\n")
            report_file.write(
                "BIRD CRITIC Stack Overflow Result Statistics (SQL Server):\n"
            )
            report_file.write(f"Number of Instances: {len(data_list)}\n")
            report_file.write(
                f"Number of Execution Errors: {number_of_execution_errors}\n"
            )
            report_file.write(f"Number of Timeouts: {number_of_timeouts}\n")
            report_file.write(
                f"Number of Assertion Errors: {number_of_assertion_errors}\n"
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
                if i < len(output_data):
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
    with open(output_jsonl_file, "w", encoding="utf-8") as f:
        for data in output_data:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

    print("Done. Output JSONL:", output_jsonl_file)


def generate_category_report(
    question_test_case_results,
    data_list,
    category_report_file,
    big_logger,
    model_name="",
    metric_name="Test Case",
):
    """
    Generates a report (txt file) summarizing success ratios across categories
    with the style

    Assumes each data_list[i] has a 'category' field: one of ["Query", "Efficiency", "Management", "Personalization"].
    Missing/invalid category defaults to "Personalization".
    """
    # Define recognized categories in a certain order
    levels = ["Query", "Management", "Personalization", "Efficiency", "Total"]
    # We'll keep track of counts and successes
    counts_dict = {
        "Query": 0,
        "Efficiency": 0,
        "Management": 0,
        "Personalization": 0,
        "Total": 0,
    }
    success_dict = {
        "Query": 0,
        "Efficiency": 0,
        "Management": 0,
        "Personalization": 0,
        "Total": 0,
    }

    # Tally up from results
    for i, q_res in enumerate(question_test_case_results):
        # Extract category from data_list; default to "Personalization" if missing or invalid.
        category = data_list[i].get("category", "Personalization")
        if category not in ["Query", "Efficiency", "Management", "Personalization"]:
            category = "Personalization"
        counts_dict[category] += 1
        counts_dict["Total"] += 1
        if q_res.get("status") == "success":
            success_dict[category] += 1
            success_dict["Total"] += 1

    # Prepare lists for printing in the style:
    # The "count" row
    count_list = [
        counts_dict["Query"],
        counts_dict["Management"],
        counts_dict["Personalization"],
        counts_dict["Efficiency"],
        counts_dict["Total"],
    ]

    # The "accuracy" row (if count == 0, ratio is 0.0)
    def ratio(success, total):
        return (success / total * 100) if total != 0 else 0.0

    score_list = [
        ratio(success_dict["Query"], counts_dict["Query"]),
        ratio(success_dict["Management"], counts_dict["Management"]),
        ratio(success_dict["Personalization"], counts_dict["Personalization"]),
        ratio(success_dict["Efficiency"], counts_dict["Efficiency"]),
        ratio(success_dict["Total"], counts_dict["Total"]),
    ]

    # Now let's write out the lines
    try:
        with open(category_report_file, "a") as rep_file:
            # Print the header (blank space + 5 levels)
            rep_file.write("{:20} {:20} {:20} {:20} {:20} {:20}\n".format("", *levels))
            # Print the count row
            rep_file.write(
                "{:20} {:<20} {:<20} {:<20} {:<20} {:<20}\n".format(
                    "count", *count_list
                )
            )
            # Separator line with metric name in the middle
            rep_file.write(
                f"===============================================    {metric_name}    ===============================================\n"
            )
            # Print the accuracy row with 2 decimals
            rep_file.write(
                "{:20} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f}\n".format(
                    "", *score_list
                )
            )
            rep_file.write(
                "================================================================================================================\n"
            )
        big_logger.info(f"Saved category report to {category_report_file}")
    except Exception as e:
        big_logger.error(f"Failed to write category report: {e}")

    csv_path = "/app/data/mssql.csv"
    file_exists = os.path.exists(csv_path)
    try:
        with open(csv_path, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            # If the file does not exist, create it and write the header.
            if not file_exists:
                writer.writerow(
                    [
                        "Model",
                        "Query",
                        "Management",
                        "Personalization",
                        "Efficiency",
                        "Total",
                    ]
                )
                # Write the current record.
                writer.writerow(
                    [
                        model_name,
                        counts_dict[0],
                        counts_dict[1],
                        counts_dict[2],
                        counts_dict[3],
                        counts_dict[4],
                    ]
                )
            # Write the current record.
            writer.writerow(
                [
                    model_name,
                    score_list[0],
                    score_list[1],
                    score_list[2],
                    score_list[3],
                    score_list[4],
                ]
            )
        big_logger.info(f"Saved CSV record to {csv_path}")
    except Exception as e:
        big_logger.error(f"Failed to write CSV record: {e}")
