#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL Database Setup and Query Execution

This module provides functions to connect to a MySQL database,
execute queries, and manage database connections, with added timeouts
and session-level maximum execution time to prevent the server from getting stuck.
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
import logging
from queue import Queue
import subprocess
import sys

import json
import re
import os, csv
from logger import PrintLogger, log_section_footer, log_section_header
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MYSQL_COMMIT_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "create",
    "drop",
    "alter",
    "truncate",
    "rename",
    "replace",
    "grant",
    "revoke",
    "lock tables",
    "unlock tables",
    "start transaction",
    "begin",
    "commit",
    "rollback",
    "call",
    "load data",
    "set",
    "do",
    "handler",
    "load xml",
    "merge",
    "prepare",
    "execute",
    "deallocate prepare",
    "xa",
)

# In PostgreSQL code, there's a dictionary _postgresql_pools. We'll mirror that:
_mysql_pools = {}

# Default config (mirroring DEFAULT_DB_CONFIG in PostgreSQL version)
DEFAULT_DB_CONFIG = {
    "minconn": 1,
    "maxconn": 5,
    "user": "root",
    "password": "123123",
    "host": "bird_critic_mysql",
    "port": 3306,
    # You can add custom timeouts or other PyMySQL connect() parameters here
}


class SimpleMySQLConnectionPool:
    """
    A minimal connection pool for PyMySQL that maintains a queue of open connections.
    """

    def __init__(self, minconn, maxconn, **conn_kwargs):
        self._conn_kwargs = conn_kwargs
        self._pool = Queue(maxsize=maxconn)
        self._maxconn = maxconn
        self._created_connections = 0

        # Pre-create 'minconn' connections
        for _ in range(minconn):
            self._pool.put(self._create_new_connection())

    def _create_new_connection(self):
        conn = pymysql.connect(**self._conn_kwargs)
        return conn

    def getconn(self):
        """
        Acquire a connection from the pool (or create a new one if pool is not full).
        """
        if not self._pool.empty():
            return self._pool.get()
        elif self._created_connections < self._maxconn:
            conn = self._create_new_connection()
            self._created_connections += 1
            return conn
        else:
            # If the pool is maxed out, block until someone returns a connection
            return self._pool.get()

    def putconn(self, conn):
        """
        Return a connection to the pool. If the pool is full, close the connection.
        """
        if self._pool.full():
            conn.close()
        else:
            self._pool.put(conn)

    def closeall(self):
        """
        Close all connections in the pool.
        """
        while not self._pool.empty():
            conn = self._pool.get()
            conn.close()


def _get_or_init_pool(db_name):
    """
    Returns a connection pool for the given database name, creating one if it does not exist.
    """
    if db_name not in _mysql_pools:
        config = DEFAULT_DB_CONFIG.copy()
        # In MySQL, database name must go under 'db' (PyMySQL parameter)
        config.update({"db": db_name})

        minconn = config.pop("minconn")
        maxconn = config.pop("maxconn")
        _mysql_pools[db_name] = SimpleMySQLConnectionPool(minconn, maxconn, **config)
    return _mysql_pools[db_name]


def perform_query_on_mysql_databases(query, db_name, conn=None):
    """
    Executes the given query on the specified MySQL database.

    1. If conn is None, we fetch a connection from the pool.
    2. If conn is provided, we reuse that connection.
    3. We automatically commit if it's a write operation (based on keywords).
    4. We return (result, conn) so the caller can reuse 'conn' for subsequent queries.

    Args:
        query (str): The SQL statement to execute.
        db_name (str): The target database name.
        conn (pymysql.connections.Connection, optional):
            An existing connection. If None, we'll get a new one from the pool.

    Returns:
        (result, connection):
            - result: The rows if it's a SELECT-like query (or non-empty fetch), else None
            - connection: The connection used (either newly acquired or reused)
    """
    MAX_ROWS = 10000  # Limit number of rows fetched to avoid huge data

    pool = _get_or_init_pool(db_name)
    need_to_put_back = False  # Whether we acquired this conn from the pool

    if conn is None:
        conn = pool.getconn()
        need_to_put_back = True

    # Attempt to set a max execution time for safety (120 seconds = 120000 ms)
    try:
        with conn.cursor() as tmp_cursor:
            tmp_cursor.execute("SET SESSION MAX_EXECUTION_TIME=120000;")
    except Exception as e:
        logger.warning(f"Could not set MAX_EXECUTION_TIME: {e}")

    cursor = conn.cursor()
    try:
        cursor.execute(query)
        lower_q = query.strip().lower()

        # Auto-commit if we find any commit keyword
        if any(kw in lower_q for kw in MYSQL_COMMIT_KEYWORDS):
            conn.commit()

        # If it starts with SELECT or WITH, we attempt to fetch up to MAX_ROWS
        if lower_q.startswith("select") or lower_q.startswith("with"):
            rows = cursor.fetchmany(MAX_ROWS + 1)
            if len(rows) > MAX_ROWS:
                rows = rows[:MAX_ROWS]
            result = rows
        else:
            # For non-select queries, try fetching in case of RETURNING or so
            try:
                result = cursor.fetchall()
            except pymysql.err.ProgrammingError:
                result = None

        return result, conn

    except Exception as e:
        conn.rollback()  # rollback on error
        raise e

    finally:
        cursor.close()
        # We do NOT put the connection back now, because the caller may want to reuse it.
        # The user explicitly calls close_mysql_connection(...) to return it to the pool.
        if need_to_put_back:
            # If you prefer to return it automatically after a single query, you could do:
            # pool.putconn(conn)
            pass


def close_mysql_connection(db_name, conn):
    """
    After the user is finished using this connection (e.g., after multiple queries),
    they call this function to release it back to the pool.
    """
    if db_name in _mysql_pools:
        pool = _mysql_pools[db_name]
        pool.putconn(conn)


def close_all_mysql_pools():
    """
    Closes all connections in all pools (e.g., at application shutdown).
    """
    for pool in _mysql_pools.values():
        pool.closeall()
    _mysql_pools.clear()


def close_mysql_pool(db_name):
    """
    Closes all connections in the specified database's pool and removes the pool reference.
    """
    if db_name in _mysql_pools:
        pool = _mysql_pools.pop(db_name)
        pool.closeall()


def get_conn(db_name):
    """
    Returns a pymysql connection for the given db_name from the connection pool.
    The caller is responsible for releasing it by calling close_mysql_connection(db_name, conn).
    """
    pool = _get_or_init_pool(db_name)
    return pool.getconn()


# -------------------------------------------------------------------
# Functions to reset ephemeral DB by "cloning" from a pre-dumped file
# -------------------------------------------------------------------
def terminate_mysql_connections(
    db_name, mysql_user, mysql_password, mysql_host, mysql_port, logger
):
    """
    Forcibly terminates all connections to `db_name` in MySQL by:
      1) SHOW PROCESSLIST
      2) KILL <process_id> for rows where the database = `db_name`
    """
    logger.info(f"Terminating all connections to MySQL DB: {db_name}")
    try:
        conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            port=mysql_port,
            database="information_schema",  # any DB is fine to run SHOW PROCESSLIST
        )
        with conn.cursor() as cur:
            cur.execute("SHOW PROCESSLIST;")
            rows = cur.fetchall()
            for row in rows:
                process_id = row[0]
                db_in_use = row[3]
                if db_in_use and db_in_use.lower() == db_name.lower():
                    kill_sql = f"KILL {process_id}"
                    logger.info(f"Killing connection for process id: {process_id}")
                    try:
                        cur.execute(kill_sql)
                    except Exception as e:
                        logger.warning(f"Failed to KILL {process_id}: {e}")
        conn.close()
    except Exception as e:
        logger.warning(f"Unable to terminate connections for {db_name}: {e}")


def reset_and_restore_database(
    ephemeral_db_name,
    base_template_db_name,
    mysql_user,
    mysql_password,
    mysql_host,
    mysql_port,
    logger=None,
):
    """
    1) Close pool for ephemeral_db_name
    2) Terminate all connections to ephemeral_db_name
    3) DROP DATABASE ephemeral_db_name
    4) CREATE DATABASE ephemeral_db_name
    5) Load ephemeral_db_name from /app/mysql_table_dumps/<base_template_db_name>_dump.sql
    """
    if logger is None:
        logger = PrintLogger()

    logger.info(
        f"Resetting ephemeral DB {ephemeral_db_name} using template {base_template_db_name}"
    )

    # 1) Close ephemeral DB pool
    logger.info(
        f"Closing connection pool for ephemeral DB {ephemeral_db_name} before resetting."
    )
    close_mysql_pool(ephemeral_db_name)

    # 2) Terminate connections
    terminate_mysql_connections(
        ephemeral_db_name, mysql_user, mysql_password, mysql_host, mysql_port, logger
    )

    # 3) DROP ephemeral DB
    drop_sql = f"DROP DATABASE IF EXISTS `{ephemeral_db_name}`"
    cmd_drop = [
        "mysql",
        f"-h{mysql_host}",
        f"-P{mysql_port}",
        f"-u{mysql_user}",
        f"-p{mysql_password}",
        "-e",
        drop_sql,
    ]
    try:
        subprocess.run(
            cmd_drop,
            check=True,
            timeout=90,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(f"Dropped DB {ephemeral_db_name} (if existed).")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error dropping DB {ephemeral_db_name}: {e}")
        sys.exit(1)

    # 4) CREATE ephemeral DB
    create_sql = f"CREATE DATABASE `{ephemeral_db_name}`"
    cmd_create = [
        "mysql",
        f"-h{mysql_host}",
        f"-P{mysql_port}",
        f"-u{mysql_user}",
        f"-p{mysql_password}",
        "-e",
        create_sql,
    ]
    try:
        subprocess.run(
            cmd_create,
            check=True,
            timeout=90,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(f"Created new DB {ephemeral_db_name}.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error creating DB {ephemeral_db_name}: {e}")
        sys.exit(1)

    # 5) Load from the pre-dumped SQL file
    #    e.g. /app/mysql_table_dumps/financial_template_dump.sql
    dump_file = f"/app/mysql_table_dumps/{base_template_db_name}_dump.sql"
    logger.info(f"Loading {ephemeral_db_name} from {dump_file}")
    load_cmd = [
        "mysql",
        f"-h{mysql_host}",
        f"-P{mysql_port}",
        f"-u{mysql_user}",
        f"-p{mysql_password}",
        ephemeral_db_name,
    ]
    try:
        with open(dump_file, "rb") as fin:
            load_proc = subprocess.run(
                load_cmd,
                stdin=fin,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=300,  # Adjust if needed for large dumps
                check=False,
            )
        if load_proc.returncode != 0:
            logger.error(
                f"Failed to load {ephemeral_db_name} from {dump_file}, "
                f"stderr:\n{load_proc.stderr.decode('utf-8', 'replace')}"
            )
            sys.exit(1)

        logger.info(f"Successfully loaded {ephemeral_db_name} from {dump_file}.")
    except subprocess.TimeoutExpired as e:
        logger.error(f"Timeout while loading {ephemeral_db_name} from {dump_file}: {e}")
        sys.exit(1)
    except FileNotFoundError:
        logger.error(f"Dump file not found: {dump_file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error loading DB: {e}")
        sys.exit(1)


def create_one_ephemeral_db(
    ephemeral_name,
    dump_path,
    mysql_host,
    mysql_port,
    mysql_user,
    mysql_password,
    logger,
):
    """
    Creates one ephemeral DB named 'ephemeral_name' and loads it from 'dump_path'.
    Returns the ephemeral_name if successful.
    """
    # 1) Drop ephemeral DB if exists
    drop_sql = f"DROP DATABASE IF EXISTS `{ephemeral_name}`"
    drop_cmd = [
        "mysql",
        f"-h{mysql_host}",
        f"-P{mysql_port}",
        f"-u{mysql_user}",
        f"-p{mysql_password}",
        "-e",
        drop_sql,
    ]
    subprocess.run(
        drop_cmd,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # 2) Create ephemeral DB
    create_sql = f"CREATE DATABASE `{ephemeral_name}`"
    create_cmd = [
        "mysql",
        f"-h{mysql_host}",
        f"-P{mysql_port}",
        f"-u{mysql_user}",
        f"-p{mysql_password}",
        "-e",
        create_sql,
    ]
    subprocess.run(
        create_cmd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # 3) Load ephemeral_name from the pre-dumped file
    try:
        with open(dump_path, "rb") as fin:
            load_cmd = [
                "mysql",
                f"-h{mysql_host}",
                f"-P{mysql_port}",
                f"-u{mysql_user}",
                f"-p{mysql_password}",
                ephemeral_name,
            ]
            load_proc = subprocess.run(
                load_cmd,
                stdin=fin,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                check=False,
            )
        if load_proc.returncode != 0:
            logger.error(
                f"Failed to load ephemeral DB {ephemeral_name} from {dump_path}, "
                f"stderr:\n{load_proc.stderr.decode('utf-8', 'replace')}"
            )
            # Return None or raise an exception; here we'll raise for clarity
            raise RuntimeError(f"Loading ephemeral DB {ephemeral_name} failed.")
    except FileNotFoundError:
        logger.error(f"Dump file not found: {dump_path}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading {ephemeral_name}: {e}")
        raise

    logger.info(
        f"Successfully created ephemeral DB '{ephemeral_name}' from {dump_path}"
    )
    return ephemeral_name


def create_ephemeral_db_copies(base_db_names, num_copies, mysql_password, logger):
    """
    Creates ephemeral DBs in parallel for each base DB:
        <base_db>_process_1, <base_db>_process_2, ...
    Loads each from /app/mysql_table_dumps/<base_db>_template_dump.sql
    """
    mysql_host = "bird_critic_mysql"
    mysql_port = 3306
    mysql_user = "root"

    ephemeral_db_pool = {}

    for base_db in base_db_names:
        template_db = base_db + "_template"  # e.g. "financial_template"
        dump_path = f"/app/mysql_table_dumps/{template_db}_dump.sql"

        ephemeral_db_pool[base_db] = []

        # We'll create N ephemeral DBs in parallel for this base_db
        futures = []
        with ThreadPoolExecutor(max_workers=num_copies) as executor:
            for i in range(1, num_copies + 1):
                ephemeral_name = f"{base_db}_process_{i}"
                logger.info(
                    f"Creating ephemeral db {ephemeral_name} from file: {dump_path}"
                )

                fut = executor.submit(
                    create_one_ephemeral_db,
                    ephemeral_name,
                    dump_path,
                    mysql_host,
                    mysql_port,
                    mysql_user,
                    mysql_password,
                    logger,
                )
                futures.append(fut)

            # Wait for all ephemeral DBs to finish
            for fut in as_completed(futures):
                # If there's an exception, as_completed will raise it here
                ephemeral_name_result = fut.result()
                ephemeral_db_pool[base_db].append(ephemeral_name_result)

        logger.info(
            f"For base_db={base_db}, ephemeral db list = {ephemeral_db_pool[base_db]}"
        )

    return ephemeral_db_pool


def execute_queries(
    queries, db_name, conn, logger=None, section_title="", is_solution=True
):
    """
    Executes a list of SQL queries on the specified connection 'conn'.
    Similar to the PostgreSQL version, but with MySQL error handling.
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
            query_result, conn = perform_query_on_mysql_databases(
                query, db_name, conn=conn
            )
            logger.info(f"Query result: {query_result}")
        except pymysql.err.OperationalError as e:
            # This could include timeouts or other operational issues
            logger.error(f"OperationalError executing query {i+1}: {e}")
            if is_solution:
                execution_error = True
            error_msg += f"\n {str(e)}"
        except pymysql.err.InternalError as e:
            logger.error(f"InternalError executing query {i+1}: {e}")
            if is_solution:
                execution_error = True
            error_msg += f"\n {str(e)}"
        except subprocess.TimeoutExpired as e:
            logger.error(f"Subprocess timeout executing query {i+1}: {e}")
            if is_solution:
                timeout_error = True
            error_msg += f"\n {str(e)}"
        except Exception as e:
            logger.error(f"Generic error executing query {i+1}: {e}")
            if is_solution:
                execution_error = True
            error_msg += f"\n {str(e)}"
        finally:
            logger.info(f"[{section_title}] DB: {db_name}, conn info: {conn}")

    log_section_footer(logger)
    return query_result, execution_error, timeout_error, error_msg


def load_jsonl(file_path):
    """
    Reads a JSONL file and returns a list of dictionaries.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return [json.loads(line) for line in file]
    except Exception as e:
        print(f"Failed to load JSONL file: {e}")
        sys.exit(1)


def split_field(data, field_name):
    """
    Retrieves a field (str or list) from 'data' and splits on '[split]' if it's a string.
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


def save_report_and_status(
    report_file_path,
    question_test_case_results,
    total_instances,
    number_of_execution_errors,
    number_of_timeouts,
    number_of_assertion_errors,
    total_errors,
    overall_accuracy,
    timestamp,
    output_data,
    base_output_folder,
    logging_enabled,
):
    """
    Writes a summary report to 'report_file_path' and, if logging_enabled is true,
    writes an updated JSONL file with status to <base_output_folder>_output_with_status.jsonl.
    """
    try:
        with open(report_file_path, "w") as report_file:
            report_file.write("--------------------------------------------------\n")
            report_file.write(
                "BIRD CRITIC Stack Overflow Result Statistics (MySQL, Multi-Thread):\n"
            )
            report_file.write(f"Number of Instances: {total_instances}\n")
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
                error_phase_note = ""
                if q_res.get("error_phase_unexpected_pass"):
                    error_phase_note = " | Error Phase: Unexpected Pass"
                sol_phase_note = ""
                if q_res.get("solution_phase_execution_error"):
                    sol_phase_note += " | Sol Phase: Execution Error"
                if q_res.get("solution_phase_timeout_error"):
                    sol_phase_note += " | Sol Phase: Timeout Error"
                if q_res.get("solution_phase_assertion_error"):
                    sol_phase_note += " | Sol Phase: Assertion Error"

                report_file.write(
                    f"Question_{q_idx}: ({t_pass}/{t_total}) test cases passed, "
                    f"failed test cases: {failed_list_str}{error_phase_note}{sol_phase_note}\n"
                )
                for output_data_item in output_data:
                    if output_data_item["instance_id"] == q_res["instance_id"]:
                        output_data_item["status"] = q_res["status"]
                        output_data_item["error_message"] = q_res["error_message"]
                        output_data_item["original_schema"] = q_res["original_schema"]
                        output_data_item["preprocess_schema"] = q_res[
                            "preprocess_schema"
                        ]

    except Exception as e:
        print(f"Failed to write report: {e}")

    print("Overall report generated:", report_file_path)

    if logging_enabled == "save":
        output_jsonl_file = f"{base_output_folder}_output_with_status.jsonl"
        with open(output_jsonl_file, "w") as f:
            for i, data in enumerate(output_data):
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
        print(f"Multithreaded results output: {output_jsonl_file}")


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

    csv_path = "/app/data/mysql.csv"
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


def performance_compare_by_qep(old_sqls, sol_sqls, db_name, conn):
    """
    MySQL approach: Use EXPLAIN FORMAT=JSON, parse some cost or row estimates, compare sums.
    """
    if not old_sqls or not sol_sqls:
        return 0

    def measure_sqls_cost(sql_list):
        total_cost = 0.0
        for sql in sql_list:
            # Attempt EXPLAIN if it looks like a DML statement
            # MySQL syntax: EXPLAIN FORMAT=JSON SELECT ...
            upper_sql = sql.strip().upper()
            if not (
                upper_sql.startswith("SELECT")
                or upper_sql.startswith("INSERT")
                or upper_sql.startswith("UPDATE")
                or upper_sql.startswith("DELETE")
                or upper_sql.startswith("WITH")
            ):
                # just run it
                try:
                    perform_query_on_mysql_databases(sql, db_name, conn)
                except Exception:
                    pass
                continue

            explain_sql = f"EXPLAIN FORMAT=JSON {sql}"
            try:
                rows, _ = perform_query_on_mysql_databases(
                    explain_sql, db_name, conn=conn
                )
                if not rows:
                    continue
                # MySQL might return 1 row with a JSON string
                # rows = [(json_text,)]
                if len(rows[0]) > 0 and rows[0][0]:
                    explain_json_str = rows[0][0]
                    try:
                        explain_data = json.loads(explain_json_str)
                        # MySQL's JSON plan might have "query_block" -> "cost_info"
                        # There's no official "Total Cost" like Postgres, but we can attempt:
                        cost_info = explain_data.get("query_block", {}).get(
                            "cost_info", {}
                        )
                        # cost_info might look like {"query_cost":"7.99"}
                        cost_val_str = cost_info.get("query_cost", "0.0")
                        total_cost += float(cost_val_str)
                    except Exception:
                        pass
            except Exception:
                pass
        return total_cost

    # Measure cost for old_sqls
    old_total_cost = measure_sqls_cost(old_sqls)
    # Measure cost for sol_sqls
    sol_total_cost = measure_sqls_cost(sol_sqls)

    return 1 if sol_total_cost < old_total_cost else 0


def get_all_databases(mysql_user, mysql_password, mysql_host, mysql_port, logger):
    """
    Get a list of all databases in MySQL server.
    """
    try:
        conn = pymysql.connect(
            host=mysql_host, user=mysql_user, password=mysql_password, port=mysql_port
        )
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES;")
            databases = [row[0] for row in cur.fetchall()]
        conn.close()
        return databases
    except Exception as e:
        logger.error(f"Failed to get database list: {e}")
        return []


def cleanup_ephemeral_databases(
    mysql_user, mysql_password, mysql_host, mysql_port, logger
):
    """
    Comprehensive cleanup of all ephemeral databases (those with '_process_' in name).
    Also handles connection termination and pool cleanup.
    """
    try:
        # 1. Get all databases
        databases = get_all_databases(
            mysql_user, mysql_password, mysql_host, mysql_port, logger
        )

        # 2. Filter for ephemeral databases
        ephemeral_dbs = [db for db in databases if "_process_" in db]

        if not ephemeral_dbs:
            logger.info("No ephemeral databases found to clean up.")
            return

        logger.info(
            f"Found {len(ephemeral_dbs)} ephemeral databases to clean up: {ephemeral_dbs}"
        )

        # 3. First terminate all connections
        for db in ephemeral_dbs:
            terminate_mysql_connections(
                db, mysql_user, mysql_password, mysql_host, mysql_port, logger
            )

            # Also clean up any connection pools for this database
            close_mysql_pool(db)

        # 4. Drop each database
        conn = pymysql.connect(
            host=mysql_host, user=mysql_user, password=mysql_password, port=mysql_port
        )

        with conn.cursor() as cur:
            for db in ephemeral_dbs:
                try:
                    logger.info(f"Dropping database: {db}")
                    cur.execute(f"DROP DATABASE IF EXISTS `{db}`")
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error dropping database {db}: {e}")

        conn.close()

        # 5. Clear all connection pools
        close_all_mysql_pools()

        logger.info("Database cleanup completed successfully")

    except Exception as e:
        logger.error(f"Error during database cleanup: {e}")
        raise


def drop_ephemeral_dbs(ephemeral_db_pool_dict, mysql_password, logger):
    """
    Enhanced version of drop_ephemeral_dbs with better error handling and cleanup.
    """
    mysql_host = "bird_critic_mysql"
    mysql_port = 3306
    mysql_user = "root"

    logger.info("=== Starting cleanup of ephemeral databases ===")

    try:
        # First cleanup all known ephemeral databases
        for base_db, ephemeral_list in ephemeral_db_pool_dict.items():
            for ephemeral_db in ephemeral_list:
                logger.info(f"Processing cleanup for: {ephemeral_db}")

                # 1. Terminate all connections
                terminate_mysql_connections(
                    ephemeral_db,
                    mysql_user,
                    mysql_password,
                    mysql_host,
                    mysql_port,
                    logger,
                )

                # 2. Close connection pool
                close_mysql_pool(ephemeral_db)

                # 3. Drop database
                try:
                    conn = pymysql.connect(
                        host=mysql_host,
                        user=mysql_user,
                        password=mysql_password,
                        port=mysql_port,
                    )
                    with conn.cursor() as cur:
                        cur.execute(f"DROP DATABASE IF EXISTS `{ephemeral_db}`")
                        conn.commit()
                    conn.close()
                except Exception as e:
                    logger.error(f"Failed to drop database {ephemeral_db}: {e}")

        # Then do a comprehensive cleanup to catch any missed databases
        cleanup_ephemeral_databases(
            mysql_user, mysql_password, mysql_host, mysql_port, logger
        )

    except Exception as e:
        logger.error(f"Error during ephemeral database cleanup: {e}")
        # Even if we get an error, try to clean up pools
        try:
            close_all_mysql_pools()
        except:
            pass
        raise
    finally:
        # Always try to clear connection pools
        try:
            close_all_mysql_pools()
        except Exception as e:
            logger.error(f"Error clearing connection pools: {e}")

    logger.info("=== Completed cleanup of ephemeral databases ===")


def enhanced_cleanup(mysql_password, logger, force=False):
    try:
        cleanup_ephemeral_databases(
            "root", mysql_password, "bird_critic_mysql", 3306, logger
        )

        if force:
            conn = pymysql.connect(
                host="bird_critic_mysql",
                user="root",
                password=mysql_password,
                port=3306,
            )
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SHOW DATABASES")
                    all_dbs = [db[0] for db in cursor.fetchall()]
                    temp_dbs = [
                        db
                        for db in all_dbs
                        if any(x in db for x in ["_process_", "_ephemeral_", "_temp_"])
                    ]

                    for db in temp_dbs:
                        logger.info(f"Force dropping database: {db}")
                        cursor.execute(f"DROP DATABASE IF EXISTS `{db}`")
                    logger.info("Purging binary logs")
                    cursor.execute("PURGE BINARY LOGS BEFORE NOW()")
            finally:
                conn.close()
    except Exception as e:
        logger.error(f"Enhanced cleanup error: {e}")
