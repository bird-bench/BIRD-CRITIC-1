#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database utility functions for Oracle operations.
"""

import os
import oracledb
import json
from logger import PrintLogger, log_section_header, log_section_footer
from datetime import datetime
import csv
import re
import time

# Common write operation keywords that will trigger a commit
ORACLE_COMMIT_KEYWORDS = (
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
    "begin",
    "commit",
    "rollback",
    "savepoint",
    "call",
    "dbms_",
)

# Default Oracle connection configuration
DEFAULT_ORACLE_CONFIG = {
    "host": "oracle19",
    "port": 1521,
    "service_name": "ORCLPDB1",
    "user": "MASTER",
    "password": "MASTER",
}


def lob_as_str_handler(cursor, name, defaultType, size, precision, scale):
    """
    Handle CLOB/NCLOB as string to prevent LOB object issues during serialization.
    """
    if defaultType == oracledb.DB_TYPE_CLOB or defaultType == oracledb.DB_TYPE_NCLOB:
        return cursor.var(str, arraysize=cursor.arraysize)
    return None


def perform_query_on_oracle_databases(query, db_name, conn=None, as_dict=False):
    """
    Execute a query on an Oracle database.

    Args:
        query (str): The SQL query to execute
        db_name (str): The database name (ephemeral user name in Oracle context)
        conn (oracledb.Connection, optional): An existing connection
        as_dict (bool): If True, return results as dictionaries instead of tuples

    Returns:
        tuple: (rows, conn) where rows is the query result and conn is the connection
    """
    if conn is None:
        # Create a connection using the provided db_name as the Oracle username
        oracle_config = DEFAULT_ORACLE_CONFIG.copy()
        oracle_config["user"] = db_name
        oracle_config["password"] = (
            db_name  # Using same value for username and password
        )

        conn = oracledb.connect(
            user=oracle_config["user"],
            password=oracle_config["password"],
            host=oracle_config["host"],
            port=oracle_config["port"],
            service_name=oracle_config["service_name"],
        )
        conn.outputtypehandler = lob_as_str_handler

    cursor = conn.cursor()
    try:
        cursor.execute(query)

        # Check if the operation is a DDL or DML operation
        query_lower = query.lower()
        is_ddl_or_dml = any(kw in query_lower for kw in ORACLE_COMMIT_KEYWORDS)
        is_query = query_lower.strip().startswith(
            "select"
        ) or query_lower.strip().startswith("with")

        # Commit for DDL/DML operations
        if is_ddl_or_dml:
            conn.commit()

        # Try to fetch results only for queries that should return rows
        if is_query:
            try:
                if as_dict and cursor.description:
                    # Convert result to list of dictionaries
                    columns = [col[0] for col in cursor.description]
                    rows = []
                    for row in cursor.fetchall():
                        rows.append(dict(zip(columns, row)))
                else:
                    rows = cursor.fetchall()
            except oracledb.DatabaseError as e:
                # For genuine errors during fetch
                rows = None
                raise e
        else:
            # For DDL/DML statements, don't try to fetch rows
            rows = None

        return rows, conn
    except Exception as e:
        query_lower = query.lower()
        is_ddl_or_dml = any(kw in query_lower for kw in ORACLE_COMMIT_KEYWORDS)
        if is_ddl_or_dml:
            conn.rollback()
        raise e
    finally:
        cursor.close()


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

    Args:
        queries (list): List of SQL queries to execute
        db_name (str): Database name (ephemeral user)
        conn (oracledb.Connection, optional): Existing database connection
        logger: Logger object
        section_title (str): Title for logging
        is_solution (bool): Whether this is a solution query (affects error handling)
        as_dict (bool): Whether to return results as dictionaries

    Returns:
        tuple: (query_result, execution_error, timeout_error)
    """
    if logger is None:
        logger = PrintLogger()

    log_section_header(section_title, logger)
    query_result = None
    execution_error = False
    timeout_error = False
    error_msg = ""
    for i, query in enumerate(queries):
        try:
            logger.info(f"Executing query {i+1}/{len(queries)}: {query}")
            query_result, conn = perform_query_on_oracle_databases(
                query, db_name, conn, as_dict=as_dict
            )

            # For queries that return data, log the result
            query_lower = query.lower().strip()
            if query_lower.startswith("select") or query_lower.startswith("with"):
                # logger.info(f"[execute_queries] Query result:: {query_result}")
                pass
            else:
                # For DDL/DML, just log success
                logger.info(f"[execute_queries] Statement executed successfully.")

        except oracledb.DatabaseError as e:
            error_msg = str(e)
            # Check if this is the "no rows returned" message for non-query statements
            if "DPY-1003" in error_msg:
                # This is expected for DDL/DML statements - not an error
                query_lower = query.lower()
                if any(kw in query_lower for kw in ORACLE_COMMIT_KEYWORDS):
                    logger.info(
                        f"[execute_queries] Statement executed successfully (no rows returned)"
                    )
                    # This is not a true error
                    continue

            # Otherwise, it's a genuine error
            logger.error(f"[execute_queries] DatabaseError executing query {i}: {e}")
            if is_solution:
                execution_error = True
            error_msg += f"\n {str(e)}"
        except Exception as e:
            logger.error(f"[execute_queries] Generic error executing query {i}: {e}")
            if is_solution:
                execution_error = True
            error_msg += f"\n {str(e)}"
        finally:
            logger.info(f"[{section_title}] DB: {db_name}, conn: {conn}")

    log_section_footer(logger)
    return query_result, execution_error, timeout_error, error_msg


def close_oracle_connection(conn):
    """
    Close a single Oracle connection.
    """
    if conn:
        conn.close()


def close_oracle_connections(conn):
    """
    Close Oracle connection(s).
    """
    close_oracle_connection(conn)


def reset_and_restore_database(db_name, logger=None):
    """
    Reset and restore an Oracle database (ephemeral user) to a known initial state.

    For Oracle, this involves:
    1. Dropping all user objects from the ephemeral schema
    2. Recreating synonyms for master tables
    """
    if logger is None:
        logger = PrintLogger()

    logger.info(f"Resetting database (user schema) [{db_name}] ...")

    admin_conn = None
    try:
        # Connect as admin (MASTER)
        admin_conn = oracledb.connect(
            user=DEFAULT_ORACLE_CONFIG["user"],
            password=DEFAULT_ORACLE_CONFIG["password"],
            host=DEFAULT_ORACLE_CONFIG["host"],
            port=DEFAULT_ORACLE_CONFIG["port"],
            service_name=DEFAULT_ORACLE_CONFIG["service_name"],
        )
        admin_conn.autocommit = True
        cursor = admin_conn.cursor()

        # Drop all objects owned by the user
        drop_objects_sql = f"""
        BEGIN
            FOR obj IN (SELECT object_name, object_type 
                       FROM all_objects 
                       WHERE owner = UPPER('{db_name}')
                       AND object_type IN ('TABLE', 'VIEW', 'PACKAGE', 
                                         'PROCEDURE', 'FUNCTION', 'SEQUENCE',
                                         'SYNONYM', 'MATERIALIZED VIEW'))
            LOOP
                BEGIN
                    IF obj.object_type = 'TABLE' THEN
                        EXECUTE IMMEDIATE 'DROP TABLE "' || UPPER('{db_name}') || '"."' || obj.object_name || '" CASCADE CONSTRAINTS';
                    ELSE
                        EXECUTE IMMEDIATE 'DROP ' || obj.object_type || ' "' || UPPER('{db_name}') || '"."' || obj.object_name || '"';
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL; -- Ignore errors and continue
                END;
            END LOOP;
        END;
        """
        logger.info(f"Dropping all objects owned by {db_name}...")
        cursor.execute(drop_objects_sql)

        # Reconnect as the ephemeral user to create synonyms
        user_config = DEFAULT_ORACLE_CONFIG.copy()
        user_config["user"] = db_name
        user_config["password"] = db_name

        user_conn = oracledb.connect(
            user=user_config["user"],
            password=user_config["password"],
            host=user_config["host"],
            port=user_config["port"],
            service_name=user_config["service_name"],
        )
        user_cursor = user_conn.cursor()

        # Create synonyms for all MASTER tables
        create_synonyms_sql = """
        BEGIN
            FOR tab IN (SELECT table_name FROM all_tables WHERE owner = 'MASTER')
            LOOP
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE OR REPLACE SYNONYM "' || tab.table_name || '" FOR MASTER."' || tab.table_name || '"';
                EXCEPTION
                    WHEN OTHERS THEN NULL;
                END;
            END LOOP;
        END;
        """
        logger.info(f"Creating synonyms for MASTER tables in {db_name}...")
        user_cursor.execute(create_synonyms_sql)
        user_conn.commit()

        # Close user connection
        user_cursor.close()
        user_conn.close()

        logger.info(f"Database (user schema) {db_name} reset successfully.")
    except Exception as e:
        logger.error(f"Error resetting database (user schema) {db_name}: {e}")
        raise
    finally:
        if admin_conn:
            admin_conn.close()


def get_connection_for_phase(db_name, logger=None):
    """
    Obtain a dedicated connection for the current phase.
    """
    if logger is None:
        logger = PrintLogger()
    logger.info(f"Acquiring dedicated connection for phase on db: {db_name}")

    oracle_config = DEFAULT_ORACLE_CONFIG.copy()
    oracle_config["user"] = db_name
    oracle_config["password"] = db_name

    try:
        conn = oracledb.connect(
            user=oracle_config["user"],
            password=oracle_config["password"],
            host=oracle_config["host"],
            port=oracle_config["port"],
            service_name=oracle_config["service_name"],
        )
        conn.outputtypehandler = lob_as_str_handler
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to {db_name}: {e}")
        return None


def execute_issue_sql(issue_sql_list, db_name, logger, conn):
    """
    Execute a list of SQL statements that are expected to raise an error.
    """
    log_section_header("Error Reproduction", logger)
    error_message = None
    issue_sql_result = None

    if not issue_sql_list:
        logger.warning("No error SQL provided for reproduction.")
    else:
        for i, query in enumerate(issue_sql_list):
            try:
                logger.info(
                    f"Executing error query {i+1}/{len(issue_sql_list)}: {query}"
                )
                query_result, _ = perform_query_on_oracle_databases(
                    query, db_name, conn
                )
            except oracledb.DatabaseError as e:
                logger.info(f"Expected error encountered for SQL {i}: {e}")
                error_message = str(e)
                break
            except Exception as e:
                logger.info(f"Expected error encountered for SQL {i}: {e}")
                error_message = str(e)
                break
            finally:
                logger.info(f"[Error SQL] DB: {db_name}, conn: {conn}")

    log_section_footer(logger)
    return error_message, issue_sql_result


def run_preprocessing(preprocess_sql, db_name, logger, conn):
    """
    If there is preprocessing SQL, execute it in sequence.
    """
    if preprocess_sql:
        execute_queries(preprocess_sql, db_name, conn, logger, "Preprocess SQL", False)


def create_ephemeral_users(base_names, num_copies, logger):
    """
    Creates ephemeral Oracle users for each base database name with ALL privileges.
    """
    ephemeral_pool = {}
    admin_conn = None
    try:
        # Connect as admin user (MASTER)
        admin_conn = oracledb.connect(
            user=DEFAULT_ORACLE_CONFIG["user"],
            password=DEFAULT_ORACLE_CONFIG["password"],
            host=DEFAULT_ORACLE_CONFIG["host"],
            port=DEFAULT_ORACLE_CONFIG["port"],
            service_name=DEFAULT_ORACLE_CONFIG["service_name"],
        )
        admin_cursor = admin_conn.cursor()

        for base in base_names:
            ephemeral_pool[base] = []
            base_upper = base.upper()

            for i in range(1, num_copies + 1):
                ephemeral_user = f"{base_upper}_PROC_{i}"

                # Try to drop the user if it exists
                drop_sql = f"DROP USER {ephemeral_user} CASCADE"
                try:
                    admin_cursor.execute(drop_sql)
                except oracledb.DatabaseError:
                    pass  # User might not exist, which is fine

                # Create the user
                create_sql = (
                    f"CREATE USER {ephemeral_user} IDENTIFIED BY {ephemeral_user}"
                )
                logger.info(f"Creating user: {ephemeral_user}")
                admin_cursor.execute(create_sql)

                # Grant all powerful privileges to the ephemeral user
                powerful_grants = [
                    f"GRANT CONNECT, RESOURCE TO {ephemeral_user}",
                    f"GRANT CREATE SESSION TO {ephemeral_user}",
                    f"GRANT UNLIMITED TABLESPACE TO {ephemeral_user}",
                    f"GRANT CREATE ANY TABLE TO {ephemeral_user}",
                    f"GRANT ALTER ANY TABLE TO {ephemeral_user}",
                    f"GRANT DROP ANY TABLE TO {ephemeral_user}",
                    f"GRANT SELECT ANY TABLE TO {ephemeral_user}",
                    f"GRANT INSERT ANY TABLE TO {ephemeral_user}",
                    f"GRANT UPDATE ANY TABLE TO {ephemeral_user}",
                    f"GRANT DELETE ANY TABLE TO {ephemeral_user}",
                    f"GRANT CREATE ANY INDEX TO {ephemeral_user}",
                    f"GRANT CREATE ANY SYNONYM TO {ephemeral_user}",
                    f"GRANT CREATE SYNONYM TO {ephemeral_user}",
                    f"GRANT CREATE PUBLIC SYNONYM TO {ephemeral_user}",
                    f"GRANT DROP ANY SYNONYM TO {ephemeral_user}",
                    f"GRANT LOCK ANY TABLE TO {ephemeral_user}",
                    f"GRANT EXECUTE ANY PROCEDURE TO {ephemeral_user}",
                    f"GRANT ALTER SESSION TO {ephemeral_user}",
                ]

                for grant_statement in powerful_grants:
                    try:
                        admin_cursor.execute(grant_statement)
                        logger.info(f"Granted privilege: {grant_statement}")
                    except oracledb.DatabaseError as e:
                        logger.warning(f"Grant failed (continuing): {e}")

                # Also grant specific privileges on MASTER tables
                grant_all_sql = f"""
                BEGIN
                    FOR rec IN (SELECT table_name FROM all_tables WHERE owner = 'MASTER') LOOP
                        BEGIN
                            EXECUTE IMMEDIATE 'GRANT ALL PRIVILEGES ON MASTER."' || rec.table_name || '" TO {ephemeral_user}';
                        EXCEPTION
                            WHEN OTHERS THEN NULL;
                        END;
                    END LOOP;
                END;"""
                logger.info(
                    f"Granting ALL PRIVILEGES on MASTER tables to {ephemeral_user}"
                )
                admin_cursor.execute(grant_all_sql)

                # Add user to ephemeral pool
                ephemeral_pool[base].append(ephemeral_user)

                # Create a connection for the ephemeral user to create synonyms
                ephemeral_cfg = DEFAULT_ORACLE_CONFIG.copy()
                ephemeral_cfg["user"] = ephemeral_user
                ephemeral_cfg["password"] = ephemeral_user

                ephemeral_conn = oracledb.connect(
                    user=ephemeral_cfg["user"],
                    password=ephemeral_cfg["password"],
                    host=ephemeral_cfg["host"],
                    port=ephemeral_cfg["port"],
                    service_name=ephemeral_cfg["service_name"],
                )
                ephemeral_cursor = ephemeral_conn.cursor()

                # Create synonyms for all MASTER tables
                try:
                    synonym_block = """
                    BEGIN
                        FOR rec IN (
                            SELECT table_name
                            FROM all_tables
                            WHERE owner = 'MASTER'
                        )
                        LOOP
                            BEGIN
                                EXECUTE IMMEDIATE
                                'CREATE OR REPLACE SYNONYM "' || rec.table_name ||
                                '" FOR MASTER."' || rec.table_name || '"';
                            EXCEPTION
                                WHEN OTHERS THEN NULL;
                            END;
                        END LOOP;
                    END;
                    """
                    logger.info(
                        f"Creating synonyms for ephemeral user: {ephemeral_user}"
                    )
                    ephemeral_cursor.execute(synonym_block)
                    ephemeral_cursor.execute("SELECT COUNT(*) FROM user_synonyms")
                    count = ephemeral_cursor.fetchone()[0]
                    logger.info(f"User {ephemeral_user} has {count} synonyms.")
                    ephemeral_conn.commit()
                finally:
                    ephemeral_cursor.close()
                    ephemeral_conn.close()

        admin_conn.commit()
    except Exception as e:
        logger.error(f"Error creating ephemeral Oracle users: {e}")
        raise
    finally:
        if admin_conn:
            admin_cursor.close()
            admin_conn.close()

    return ephemeral_pool


def drop_ephemeral_users(ephemeral_pool, logger):
    """
    Drop ephemeral users after testing is complete.

    Args:
        ephemeral_pool (dict): Mapping from base names to lists of ephemeral user names
        logger: Logger object
    """
    admin_conn = None
    try:
        admin_conn = oracledb.connect(
            user=DEFAULT_ORACLE_CONFIG["user"],
            password=DEFAULT_ORACLE_CONFIG["password"],
            host=DEFAULT_ORACLE_CONFIG["host"],
            port=DEFAULT_ORACLE_CONFIG["port"],
            service_name=DEFAULT_ORACLE_CONFIG["service_name"],
        )
        admin_cursor = admin_conn.cursor()

        for base, user_list in ephemeral_pool.items():
            for user in user_list:
                drop_sql = f"DROP USER {user} CASCADE"
                logger.info(f"Dropping ephemeral user: {user}")
                try:
                    admin_cursor.execute(drop_sql)
                except oracledb.DatabaseError as e:
                    logger.warning(f"Failed to drop user {user}: {e}")

        admin_conn.commit()
    except Exception as e:
        logger.error(f"Error dropping ephemeral users: {e}")
    finally:
        if admin_conn:
            admin_cursor.close()
            admin_conn.close()


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

    csv_path = "/app/data/oracle.csv"
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


def generate_report_and_output(
    jsonl_file,
    data_list,
    error_messages,
    question_test_case_results,
    number_of_execution_errors,
    number_of_timeouts,
    number_of_assertion_errors,
    total_passed_instances,
    save_status_file=True,
):
    """
    Generate the final report and output JSONL with status.
    """
    total_instances = len(data_list)
    total_errors = (
        number_of_execution_errors + number_of_timeouts + number_of_assertion_errors
    )
    total_passed_instances_wo_error_pass = total_passed_instances
    total_passed_instances_wo_error_pass = max(total_passed_instances_wo_error_pass, 0)
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
                "BIRD CRITIC Stack Overflow Result Statistics (Oracle):\n"
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
                    output_data[i]["original_schema"] = q_res["original_schema"]
                    output_data[i]["preprocess_schema"] = q_res["preprocess_schema"]
    except Exception as e:
        print(f"Failed to write report: {e}")

    print("Overall report generated:", report_file_path)

    if save_status_file:
        output_jsonl_file = f"{base_output_folder}_with_status.jsonl"
        try:
            with open(output_jsonl_file, "w", encoding="utf-8") as jsonl_file:
                for data in output_data:
                    jsonl_file.write(json.dumps(data, ensure_ascii=False) + "\n")
            print("Status saved to:", output_jsonl_file)
        except Exception as e:
            print(f"Failed to save status: {e}")


def cleanup_all_ephemeral_users(logger, force=False):
    """
    Find and drop all ephemeral users that might have been created during evaluation.
    This is a safety measure to ensure no leftover users remain.

    Args:
        logger: Logger instance
        force: If True, attempt more aggressive cleanup methods
    """
    try:
        # Connect as MASTER user (admin)
        conn = oracledb.connect(
            user=DEFAULT_ORACLE_CONFIG["user"],
            password=DEFAULT_ORACLE_CONFIG["password"],
            host=DEFAULT_ORACLE_CONFIG["host"],
            port=DEFAULT_ORACLE_CONFIG["port"],
            service_name=DEFAULT_ORACLE_CONFIG["service_name"],
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Find all ephemeral users (pattern: ORIG_DB_PROC_xxx)
        cursor.execute(
            "SELECT username FROM all_users WHERE username LIKE '%\\_PROC\\_%' ESCAPE '\\'"
        )
        ephemeral_users = [row[0] for row in cursor.fetchall()]

        if ephemeral_users:
            logger.info(
                f"Found {len(ephemeral_users)} leftover ephemeral users: {ephemeral_users}"
            )

            for username in ephemeral_users:
                try:
                    # Kill all sessions for this user
                    if force:
                        cursor.execute(
                            f"SELECT sid, serial# FROM v$session WHERE username = '{username}'"
                        )
                        sessions = cursor.fetchall()
                        for sid, serial in sessions:
                            try:
                                logger.info(
                                    f"Killing session {sid},{serial} for user {username}"
                                )
                                cursor.execute(
                                    f"ALTER SYSTEM KILL SESSION '{sid},{serial}' IMMEDIATE"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Error killing session for {username}: {e}"
                                )

                    # Wait a moment for sessions to be terminated
                    time.sleep(0.5)

                    # Drop the user with CASCADE
                    logger.info(f"Dropping ephemeral user {username}")
                    cursor.execute(f"DROP USER {username} CASCADE")

                except Exception as e:
                    logger.error(f"Error dropping ephemeral user {username}: {e}")
                    if force:
                        try:
                            # Try with FORCE option if available
                            cursor.execute(f"DROP USER {username} CASCADE")
                        except Exception as force_error:
                            logger.error(
                                f"Force drop also failed for {username}: {force_error}"
                            )
        else:
            logger.info("No leftover ephemeral users found.")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error during comprehensive cleanup: {e}")

        # If force is enabled, try even more aggressive cleanup
        if force:
            try:
                logger.info("Attempting emergency cleanup...")
                emergency_cleanup(logger)
            except Exception as emergency_e:
                logger.error(f"Emergency cleanup also failed: {emergency_e}")


def emergency_cleanup(logger):
    """
    Last resort cleanup method to terminate stuck sessions and
    free up resources.

    Args:
        logger: Logger instance
    """
    try:
        # Connect as MASTER user (admin)
        conn = oracledb.connect(
            user=DEFAULT_ORACLE_CONFIG["user"],
            password=DEFAULT_ORACLE_CONFIG["password"],
            host=DEFAULT_ORACLE_CONFIG["host"],
            port=DEFAULT_ORACLE_CONFIG["port"],
            service_name=DEFAULT_ORACLE_CONFIG["service_name"],
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Find sessions that might be related to evaluation
        cursor.execute(
            "SELECT sid, serial#, username, status FROM v$session "
            "WHERE username IS NOT NULL AND username != 'SYS' AND username != 'SYSTEM' "
            "AND username != 'MASTER'"
        )

        sessions = cursor.fetchall()
        killed_count = 0

        for sid, serial, username, status in sessions:
            if username and (re.search(r"_PROC_", username) or status == "KILLED"):
                try:
                    logger.info(
                        f"Emergency killing session {sid},{serial} for user {username}"
                    )
                    cursor.execute(
                        f"ALTER SYSTEM KILL SESSION '{sid},{serial}' IMMEDIATE"
                    )
                    killed_count += 1
                except Exception as e:
                    logger.error(
                        f"Error in emergency kill for session {sid},{serial}: {e}"
                    )

        logger.info(f"Emergency cleanup killed {killed_count} sessions")

        # Flush shared pool as a last resort to free memory
        try:
            cursor.execute("ALTER SYSTEM FLUSH SHARED_POOL")
            logger.info("Flushed shared pool")
        except Exception as e:
            logger.error(f"Error flushing shared pool: {e}")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Emergency cleanup failed: {e}")


def cleanup_orphaned_objects(logger):
    """
    Cleanup any orphaned objects left by dropped users.
    This can help recover space and prevent issues with future tests.

    Args:
        logger: Logger instance
    """
    try:
        conn = oracledb.connect(
            user=DEFAULT_ORACLE_CONFIG["user"],
            password=DEFAULT_ORACLE_CONFIG["password"],
            host=DEFAULT_ORACLE_CONFIG["host"],
            port=DEFAULT_ORACLE_CONFIG["port"],
            service_name=DEFAULT_ORACLE_CONFIG["service_name"],
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Purge recyclebin
        try:
            logger.info("Purging recyclebin...")
            cursor.execute("PURGE DBA_RECYCLEBIN")
        except Exception as e:
            logger.error(f"Error purging recyclebin: {e}")

        # Cleanup temporary tables
        try:
            logger.info("Cleaning up temporary tables...")
            cursor.execute(
                """
                BEGIN
                    FOR obj IN (
                        SELECT * FROM dba_objects 
                        WHERE object_type = 'TABLE' 
                        AND temporary = 'Y'
                        AND owner LIKE '%\_PROC\_%' ESCAPE '\\'
                    ) LOOP
                        BEGIN
                            EXECUTE IMMEDIATE 'DROP TABLE ' || obj.owner || '.' || obj.object_name || ' PURGE';
                        EXCEPTION
                            WHEN OTHERS THEN NULL;
                        END;
                    END LOOP;
                END;
                """
            )
        except Exception as e:
            logger.error(f"Error cleaning up temporary tables: {e}")

        # Close database resources
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error in orphaned objects cleanup: {e}")
