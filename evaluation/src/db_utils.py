# db_utils.py
import os
import subprocess
import psycopg2
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool
from logger import (
    log_section_header,
    log_section_footer,
    PrintLogger
)


_postgresql_pools = {}

DEFAULT_DB_CONFIG = {
    "minconn": 1,
    "maxconn": 5,
    "user": "root",
    "password": "123123",
    "host": "bird_critic_postgresql",
    "port": 5432,
}

def _get_or_init_pool(db_name):
    """
    Returns a connection pool for the given database name, creating one if it does not exist.
    """
    if db_name not in _postgresql_pools:
        config = DEFAULT_DB_CONFIG.copy()
        config.update({"dbname": db_name})
        _postgresql_pools[db_name] = SimpleConnectionPool(
            config["minconn"],
            config["maxconn"],
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
        )
    return _postgresql_pools[db_name]

def perform_query_on_postgresql_databases(query, db_name, conn=None):
    """
    Executes the given query on the specified database, returns (result, conn).
    Automatically commits if the query is recognized as a write operation.
    """
    MAX_ROWS = 10000
    pool = _get_or_init_pool(db_name)
    need_to_put_back = False

    if conn is None:
        conn = pool.getconn()
        need_to_put_back = True

    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '60s';")  # 60s query timeout

    try:
        cursor.execute(query)
        lower_q = query.strip().lower()
        conn.commit()

        if lower_q.startswith("select") or lower_q.startswith("with"):
            # Fetch up to MAX_ROWS + 1 to see if there's an overflow
            rows = cursor.fetchmany(MAX_ROWS + 1)
            if len(rows) > MAX_ROWS:
                rows = rows[:MAX_ROWS]
            result = rows
        else:
            try:
                result = cursor.fetchall()
            except psycopg2.ProgrammingError:
                result = None

        return (result, conn)

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        if need_to_put_back:
            # If you only need a single query, you could return it right away:
            # But usually, we keep the same conn for subsequent queries, so do nothing.
            # If you truly do not want to reuse it, uncomment below:
            # pool.putconn(conn)
            pass

def close_postgresql_connection(db_name, conn):
    """
    Release a connection back to the pool when you are done with it.
    """
    if db_name in _postgresql_pools:
        pool = _postgresql_pools[db_name]
        pool.putconn(conn)

def close_all_postgresql_pools():
    """
    Closes all connections in all pools (e.g., at application shutdown).
    """
    for pool in _postgresql_pools.values():
        pool.closeall()
    _postgresql_pools.clear()

def close_postgresql_pool(db_name):
    """
    Close the pool for a specific db_name and remove its reference.
    """
    if db_name in _postgresql_pools:
        pool = _postgresql_pools.pop(db_name)
        pool.closeall()

def get_connection_for_phase(db_name, logger):
    """
    Acquire a new connection (borrowed from the connection pool) for a specific phase.
    """
    logger.info(f"Acquiring dedicated connection for phase on db: {db_name}")
    result, conn = perform_query_on_postgresql_databases("SELECT 1", db_name, conn=None)
    return conn

def reset_and_restore_database(db_name, pg_password, logger):
    """
    Resets the database by dropping it and re-creating it from its template.
    1) close pool
    2) terminate connections
    3) dropdb
    4) createdb --template ...
    """
    pg_host = "bird_critic_postgresql"
    pg_port = 5432
    pg_user = "root"

    env_vars = os.environ.copy()
    env_vars["PGPASSWORD"] = pg_password
    base_db_name = db_name.split('_process_')[0]
    template_db_name = f"{base_db_name}_template"

    logger.info(f"Resetting database {db_name} using template {template_db_name}")

    # 1) Close the pool
    logger.info(f"Closing connection pool for database {db_name} before resetting.")
    close_postgresql_pool(db_name)

    # 2) Terminate existing connections
    terminate_command = [
        "psql",
        "-h", pg_host,
        "-p", str(pg_port),
        "-U", pg_user,
        "-d", "postgres",
        "-c",
        f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = '{db_name}' AND pid <> pg_backend_pid();
        """
    ]
    subprocess.run(
        terminate_command,
        check=True,
        env=env_vars,
        timeout=60,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    logger.info(f"All connections to database {db_name} have been terminated.")

    # 3) dropdb
    drop_command = [
        "dropdb",
        "--if-exists",
        "-h", pg_host,
        "-p", str(pg_port),
        "-U", pg_user,
        db_name,
    ]
    subprocess.run(drop_command, check=True, env=env_vars, timeout=60,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info(f"Database {db_name} dropped if it existed.")

    # 4) createdb --template=xxx_template
    create_command = [
        "createdb",
        "-h", pg_host,
        "-p", str(pg_port),
        "-U", pg_user,
        db_name,
        "--template", template_db_name
    ]
    subprocess.run(create_command, check=True, env=env_vars, timeout=60,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info(f"Database {db_name} created from template {template_db_name} successfully.")

def create_ephemeral_db_copies(base_db_names, num_copies, pg_password, logger):
    """
    For each base database in base_db_names, create `num_copies` ephemeral DB copies 
    from base_db_template. Return a dict: {base_db: [ephemeral1, ephemeral2, ...], ...}
    """
    pg_host = "bird_critic_postgresql"
    pg_port = 5432
    pg_user = "root"
    env_vars = os.environ.copy()
    env_vars["PGPASSWORD"] = pg_password

    ephemeral_db_pool = {}

    for base_db in base_db_names:
        base_template = f"{base_db}_template"
        ephemeral_db_pool[base_db] = []

        for i in range(1, num_copies+1):
            ephemeral_name = f"{base_db}_process_{i}"
            # If it already exists, drop it first
            drop_cmd = [
                "dropdb", "--if-exists",
                "-h", pg_host,
                "-p", str(pg_port),
                "-U", pg_user,
                ephemeral_name
            ]
            subprocess.run(drop_cmd, check=False, env=env_vars,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            # createdb
            create_cmd = [
                "createdb",
                "-h", pg_host,
                "-p", str(pg_port),
                "-U", pg_user,
                ephemeral_name,
                "--template", base_template
            ]
            logger.info(f"Creating ephemeral db {ephemeral_name} from {base_template}...")
            subprocess.run(create_cmd, check=True, env=env_vars,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            ephemeral_db_pool[base_db].append(ephemeral_name)

        logger.info(f"For base_db={base_db}, ephemeral db list = {ephemeral_db_pool[base_db]}")

    return ephemeral_db_pool

def drop_ephemeral_dbs(ephemeral_db_pool_dict, pg_password, logger):
    """
    Delete all ephemeral databases created during the script execution.
    """
    pg_host = "bird_critic_postgresql"
    pg_port = 5432
    pg_user = "root"
    env_vars = os.environ.copy()
    env_vars["PGPASSWORD"] = pg_password

    logger.info("=== Cleaning up ephemeral databases ===")
    for base_db, ephemeral_list in ephemeral_db_pool_dict.items():
        for ephemeral_db in ephemeral_list:
            logger.info(f"Dropping ephemeral db: {ephemeral_db}")
            drop_cmd = [
                "dropdb",
                "--if-exists",
                "-h", pg_host,
                "-p", str(pg_port),
                "-U", pg_user,
                ephemeral_db
            ]
            try:
                subprocess.run(drop_cmd, check=True, env=env_vars,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to drop ephemeral db {ephemeral_db}: {e}")


def execute_queries(queries, db_name, conn, logger=None, section_title=""):
    """
    Execute a list of queries using the SAME connection (conn).
    Returns (query_result, execution_error_flag, timeout_flag).
    Once the first error occurs, we break out and return immediately.
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
            query_result, conn = perform_query_on_postgresql_databases(query, db_name, conn=conn)
            logger.info(f"Query result: {query_result}")

        except psycopg2.errors.QueryCanceled as e:
            # Timeout error
            logger.error(f"Timeout error executing query {i+1}: {e}")
            timeout_error = True
            break

        except OperationalError as e:
            # Operational errors (e.g., server not available, etc.)
            logger.error(f"OperationalError executing query {i+1}: {e}")
            execution_error = True
            break

        except psycopg2.Error as e:
            # Other psycopg2 errors (e.g., syntax errors, constraint violations)
            logger.error(f"psycopg2 Error executing query {i+1}: {e}")
            execution_error = True
            break

        except Exception as e:
            # Any other generic error
            logger.error(f"Generic error executing query {i+1}: {e}")
            execution_error = True
            break

        finally:
            logger.info(f"[{section_title}] DB: {db_name}, conn info: {conn}")

        # If an error is flagged, don't continue subsequent queries
        if execution_error or timeout_error:
            break

    log_section_footer(logger)
    return query_result, execution_error, timeout_error