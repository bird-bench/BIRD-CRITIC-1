# db_utils.py
import os
import subprocess
import sqlite3
import shutil
import signal
import threading
import time

try:
    from .logger import log_section_header, log_section_footer, PrintLogger, NullLogger
except ImportError:
    from logger import log_section_header, log_section_footer, PrintLogger, NullLogger


def perform_query_on_sqlite_databases(query, db_path, conn=None, query_timeout=30):
    """
    Execute query on specified SQLite database, return (result, conn).
    Skip complex nested queries directly to avoid deadlock.

    Parameters:
        query_timeout: Timeout for single query (seconds), default 15 seconds
    """
    MAX_ROWS = 10000
    need_to_close = False

    # Check query complexity, skip complex queries directly
    lower_q = query.strip().lower()
    if conn is None:
        # CRITICAL: Check file exists before connecting!
        # sqlite3.connect() will CREATE an empty database if file doesn't exist!
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found: {db_path}")

        # Use URI mode to prevent accidental database creation
        conn = sqlite3.connect(f"file:{db_path}?mode=rw", uri=True, timeout=query_timeout)
        conn.execute(f"PRAGMA busy_timeout = {query_timeout * 1000}")  # Set busy wait timeout to query_timeout seconds
        need_to_close = True

    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = OFF")
    
    cursor = conn.cursor()

    try:
        
        start_time = time.time()
        cursor.execute(query)
        
        if lower_q.startswith(('select', 'with')):
            result = cursor.fetchall()
            if result and len(result) > MAX_ROWS:
                result = result[:MAX_ROWS]
        else:
            conn.commit()
            try:
                result = cursor.fetchall()
            except Exception:
                result = None

        
        return (result, conn)

    except sqlite3.OperationalError as e:
        # print(f"[ERROR] SQLite OperationalError: {e}")
        conn.rollback()
        raise e
    except Exception as e:
        # print(f"[ERROR] Query failed: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        if need_to_close:
            pass


def execute_queries(
    queries, db_path, conn, logger=None, section_title="", return_error=False
):
    """
    Execute query list using same connection, skip complex queries directly
    """
    if logger is None:
        logger = NullLogger()

    log_section_header(section_title, logger)
    query_result = None
    execution_error = False
    timeout_error = False

    error_message = ""
    if isinstance(queries, str):
        queries = [queries]

    for i, query in enumerate(queries):
        try:
            logger.info(f"Executing query {i+1}/{len(queries)}: {query[:100]}... on {db_path}")
            
            query_result, conn = perform_query_on_sqlite_databases(
                query, db_path, conn=conn, query_timeout=30
            )

        except sqlite3.OperationalError as e:
            error_str = str(e).lower()
            if "database is locked" in error_str or "timeout" in error_str or "complex query skipped" in error_str:
                logger.error(f"Timeout/Skip error executing query {i+1}: {e}")
                error_message += f"Timeout/Skip error executing query {i+1}: {e}\n"
                timeout_error = True
            else:
                logger.error(f"OperationalError executing query {i+1}: {e}")
                error_message += f"OperationalError executing query {i+1}: {e}\n"
                execution_error = True
            break

        except sqlite3.Error as e:
            logger.error(f"SQLite Error executing query {i+1}: {e}")
            error_message += f"SQLite Error executing query {i+1}: {e}\n"
            execution_error = True
            break

        except Exception as e:
            logger.error(f"Generic error executing query {i+1}: {e}")
            error_message += f"Generic error executing query {i+1}: {e}\n"
            execution_error = True
            break

        finally:
            logger.info(f"[{section_title}] DB: {db_path}, conn info: {conn}")

        if execution_error or timeout_error:
            break

    log_section_footer(logger)
    if return_error:
        return query_result, execution_error, timeout_error, error_message
    else:
        return query_result, execution_error, timeout_error



def close_sqlite_connection(db_path, conn):
    """
    Close SQLite connection with thorough cleanup

    IMPROVEMENTS:
    - Commit any pending transactions
    - Close all cursors
    - Wait for file system sync
    """
    if conn:
        try:
            # Commit any pending transactions
            conn.commit()
        except Exception as e:
            # Rollback if commit fails
            try:
                conn.rollback()
            except:
                pass

        try:
            # Close the connection
            conn.close()

            # IMPROVEMENT: Wait for file system to sync (2 seconds)
            # time.sleep(1.0)
        except Exception as e:
            # Log but don't raise - connection may already be closed
            pass


def get_connection_for_phase(db_path, logger):
    """
    Get new connection for specific phase
    """
    logger.info(f"Acquiring dedicated connection for phase on db: {db_path}")
    result, conn = perform_query_on_sqlite_databases("SELECT 1", db_path, conn=None)
    return conn


def reset_and_restore_database(db_path, pg_password, logger):
    """
    Reset database by copying from template
    """
    # Original logic: extract "alien" from "alien_ephemeral_1.sqlite"
    # This line has issue:
    # base_db_name = os.path.basename(db_path).replace(".sqlite", "").split("_process_")[0]

    # Fix: correctly extract base database name
    filename = os.path.basename(db_path)  # alien_ephemeral_1.sqlite
    base_db_name = filename.replace(".sqlite", "")  # alien_ephemeral_1

    # If it's ephemeral database, extract base name
    if "_ephemeral_" in base_db_name:
        base_db_name = base_db_name.split("_ephemeral_")[0]  # alien
    elif "_process_" in base_db_name:
        base_db_name = base_db_name.split("_process_")[0]

    template_db_path = os.path.join(
        os.path.dirname(db_path), f"{base_db_name}_template.sqlite"
    )

    logger.info(f"Resetting database {db_path} using template {template_db_path}")
    logger.info(f"Base DB name extracted: {base_db_name}")

    # Check if template file exists
    if not os.path.exists(template_db_path):
        logger.error(f"Template database not found: {template_db_path}")

        # List all files in directory to help debug
        db_dir = os.path.dirname(db_path)
        try:
            files_in_dir = os.listdir(db_dir)
            logger.info(f"Files in directory {db_dir}: {files_in_dir}")

            # Look for possible template files
            template_files = [f for f in files_in_dir if f.endswith("_template.sqlite")]
            if template_files:
                # Use first template file found
                template_db_path = os.path.join(db_dir, template_files[0])
                logger.info(f"Using found template file: {template_db_path}")
            else:
                # If no template file, look for main database file
                main_db_files = [
                    f for f in files_in_dir if f == f"{base_db_name}.sqlite"
                ]
                if main_db_files:
                    main_db_path = os.path.join(db_dir, main_db_files[0])
                    logger.info(
                        f"No template found, using main database: {main_db_path}"
                    )
                    template_db_path = main_db_path
                else:
                    raise FileNotFoundError(
                        f"No suitable database file found in {db_dir}"
                    )
        except Exception as e:
            logger.error(f"Error listing directory {db_dir}: {e}")
            raise

    # IMPROVEMENT 1: Thorough cleanup of WAL/SHM files BEFORE deleting database
    wal_files = [db_path + '-wal', db_path + '-shm', db_path + '-journal']
    for wal_file in wal_files:
        if os.path.exists(wal_file):
            try:
                os.remove(wal_file)
                logger.info(f"Removed WAL/SHM file: {wal_file}")
            except Exception as e:
                logger.warning(f"Failed to remove {wal_file}: {e}")

    # IMPROVEMENT 2: Increased sleep time for file system sync (2 seconds)
    # time.sleep(1.0)

    # 1) Delete existing database with retry logic
    if os.path.exists(db_path):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                os.remove(db_path)
                logger.info(f"Database {db_path} removed.")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 0.5 * (2 ** attempt)  # Exponential backoff: 1.0, 2.0, 4.0, 8.0
                    logger.warning(f"Failed to remove database (attempt {attempt+1}/{max_retries}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to remove database after {max_retries} attempts: {e}")
                    raise

    # IMPROVEMENT 3: Additional sleep before copy to prevent disk I/O errors (2 seconds)
    # time.sleep(1.0)

    # 2) Copy from template with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            shutil.copy2(template_db_path, db_path)
            logger.info(
                f"Database {db_path} created from template {template_db_path} successfully."
            )
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 0.5 * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Failed to copy database (attempt {attempt+1}/{max_retries}): {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to copy database after {max_retries} attempts: {e}")
                raise

    # IMPROVEMENT 4: Final sleep to ensure file system sync (2 seconds)
    # time.sleep(1.0)


def create_ephemeral_db_copies(base_db_names, num_copies, pg_password, logger, db_dir="./sqlite_databases_test"):
    """
    Create num_copies ephemeral copies for each base database
    Return dictionary: {base_db: [ephemeral1_path, ephemeral2_path, ...], ...}

    Parameters:
        base_db_names: List of base database names
        num_copies: Number of ephemeral copies to create
        pg_password: Password (kept for compatibility, not used for SQLite)
        logger: Logger instance
        db_dir: Path to the directory containing SQLite databases
    """
    ephemeral_db_pool = {}

    for base_db in base_db_names:
        db_path = db_dir
        base_template_path = f"{db_path}/{base_db}/{base_db}_template.sqlite"

        # Check and debug path
        logger.info(f"Looking for template: {base_template_path}")

        if not os.path.exists(base_template_path):
            logger.warning(f"Template database not found: {base_template_path}")

            # Debug: list directory content
            current_db_dir = f"{db_path}/{base_db}"
            if os.path.exists(current_db_dir):
                files = os.listdir(current_db_dir)
                logger.info(f"Files in {current_db_dir}: {files}")

                # Look for template files
                template_candidates = [
                    f for f in files if "template" in f and f.endswith(".sqlite")
                ]
                if template_candidates:
                    base_template_path = os.path.join(current_db_dir, template_candidates[0])
                    logger.info(f"Using template candidate: {base_template_path}")
                else:
                    # Look for main database file
                    main_db_candidates = [f for f in files if f == f"{base_db}.sqlite"]
                    if main_db_candidates:
                        base_template_path = os.path.join(current_db_dir, main_db_candidates[0])
                        logger.info(
                            f"Using main database as template: {base_template_path}"
                        )
                    else:
                        logger.error(f"No suitable database file found for {base_db}")
                        continue
            else:
                logger.error(f"Database directory does not exist: {current_db_dir}")
                continue

        ephemeral_db_pool[base_db] = []

        for i in range(1, num_copies + 1):
            ephemeral_name = f"{db_path}/{base_db}/{base_db}_ephemeral_{i}.sqlite"

            # If already exists, delete first
            if os.path.exists(ephemeral_name):
                os.remove(ephemeral_name)

            # Copy from template
            logger.info(
                f"Creating ephemeral db {ephemeral_name} from {base_template_path}..."
            )
            try:
                shutil.copy2(base_template_path, ephemeral_name)
                ephemeral_db_pool[base_db].append(ephemeral_name)
                logger.info(f"Successfully created {ephemeral_name}")
            except Exception as e:
                logger.error(f"Failed to create ephemeral db {ephemeral_name}: {e}")

        logger.info(
            f"For base_db={base_db}, ephemeral db list = {ephemeral_db_pool[base_db]}"
        )

    return ephemeral_db_pool


def drop_ephemeral_dbs(ephemeral_db_pool_dict, pg_password, logger):
    """
    Delete all ephemeral databases created during script execution
    """
    logger.info("=== Cleaning up ephemeral databases ===")
    for base_db, ephemeral_list in ephemeral_db_pool_dict.items():
        for ephemeral_db_path in ephemeral_list:
            if os.path.exists(ephemeral_db_path):
                logger.info(f"Dropping ephemeral db: {ephemeral_db_path}")
                try:
                    os.remove(ephemeral_db_path)
                except Exception as e:
                    logger.error(
                        f"Failed to drop ephemeral db {ephemeral_db_path}: {e}"
                    )


def cleanup_all_database_files(db_dir, logger):
    """
    Clean up all files except {db_id}_template.sqlite in each database subdirectory
    This includes ephemeral databases, WAL files, SHM files, and other temporary files

    IMPROVEMENTS:
    - Retry logic with exponential backoff for locked files
    - Force sync after cleanup
    - Better error handling

    Args:
        db_dir (str): Path to the database directory
        logger: Logger instance

    Returns:
        tuple: (files_deleted, total_size_freed)
    """
    logger.info("=== Starting comprehensive database cleanup ===")

    if not os.path.exists(db_dir):
        logger.error(f"Database directory does not exist: {db_dir}")
        return 0, 0

    files_deleted = 0
    total_size_freed = 0
    files_failed = 0

    # IMPROVEMENT: Wait before cleanup to let any pending operations finish
    # time.sleep(1.0)

    # Iterate through all subdirectories
    for db_name in os.listdir(db_dir):
        db_subdir = os.path.join(db_dir, db_name)

        # Skip if not a directory
        if not os.path.isdir(db_subdir):
            continue

        logger.info(f"Cleaning database directory: {db_name}")

        # Get the expected template file name
        template_file = f"{db_name}_template.sqlite"

        # List all files in the subdirectory
        files_to_delete = []
        for filename in os.listdir(db_subdir):
            file_path = os.path.join(db_subdir, filename)

            # Skip if not a file
            if not os.path.isfile(file_path):
                continue

            # Keep only the template file
            if filename == template_file:
                continue

            files_to_delete.append((filename, file_path))

        # IMPROVEMENT: Delete files with retry logic
        for filename, file_path in files_to_delete:
            file_size = 0
            deleted = False

            # Try to get file size before deletion
            try:
                file_size = os.path.getsize(file_path)
            except Exception as e:
                logger.warning(f"  Could not get size of {filename}: {e}")

            # Delete with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    os.remove(file_path)
                    logger.info(f"  Deleted: {filename} ({file_size} bytes)")
                    files_deleted += 1
                    total_size_freed += file_size
                    deleted = True
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = 0.5 * (2 ** attempt)  # 0.5, 1.0, 2.0 seconds
                        logger.warning(f"  Failed to delete {filename} (attempt {attempt+1}/{max_retries}): {e}")
                        logger.info(f"  Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"  Failed to delete {filename} after {max_retries} attempts: {e}")
                        files_failed += 1

    # IMPROVEMENT: Force file system sync after cleanup
    try:
        os.sync()
        logger.info("File system sync completed")
    except:
        pass

    # Wait for sync to complete
    # time.sleep(1.0)

    # Format size for logging
    size_mb = total_size_freed / (1024 * 1024)
    logger.info(f"=== Cleanup completed: {files_deleted} files deleted, {files_failed} failed, {size_mb:.2f} MB freed ===")

    return files_deleted, total_size_freed