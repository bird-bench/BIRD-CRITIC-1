#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL Database Setup and Query Execution

This module provides functions to connect to a MySQL database,
execute queries, and manage database connections, with added timeouts
and session-level maximum execution time to prevent the server from getting stuck.
"""

import pymysql
from pymysql.constants import CLIENT
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

mysql_commit_keywords = (
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


def connect_mysql(db_name):
    """
    Establish a connection to the MySQL database with timeouts.

    Args:
        db_name (str): The name of the database to connect to.

    Returns:
        pymysql.connections.Connection: A connection object to the database.
    """
    db = pymysql.connect(
        host="bird_critic_mysql",
        user="root",
        password="123123",
        database=db_name,
        port=3306,
        connect_timeout=120,  # Connection timeout in seconds
        read_timeout=120,  # Read timeout in seconds
        write_timeout=120,  # Write timeout in seconds
        client_flag=CLIENT.MULTI_STATEMENTS,  # Allow multiple statements in one query
    )
    # Set session-level max execution time (in milliseconds) to prevent long-running queries
    try:
        with db.cursor() as cursor:
            # 90000 ms = 90 seconds
            cursor.execute("SET SESSION MAX_EXECUTION_TIME=120000;")
    except Exception as e:
        logger.warning(f"Could not set MAX_EXECUTION_TIME: {e}")

    return db


def execute_mysql_query(cursor, query, commit=False):
    """
    Execute a MySQL query and optionally commit changes.

    Args:
        cursor (pymysql.cursors.Cursor): The database cursor.
        query (str): The SQL query to execute.
        commit (bool, optional): Whether to commit changes. Defaults to False.

    Returns:
        list or None: The query results if any, None for write operations.
    """
    cursor.execute(query)
    if commit:
        cursor.connection.commit()
    try:
        result = cursor.fetchall()
    except pymysql.err.ProgrammingError:
        # No results to fetch (e.g., for INSERT queries)
        result = None
    return result


def perform_query_on_mysql_databases(query, db_name):
    """
    Perform a query on a MySQL database and keep the connection open.

    This function will:
        - Open a connection with timeouts
        - Determine if the query is a write operation (DML/DDL) by checking keywords
        - Execute the query with a max execution time
        - Commit if it's a write operation
        - Return the result and the open database connection
          (Caller is responsible for closing the connection)

    Args:
        query (str): The SQL query to execute.
        db_name (str): The name of the database to query.

    Returns:
        tuple: (result, db)
            result: The query results if any, None for write operations.
            db:    The open database connection. The caller must close it.
    """
    db = connect_mysql(db_name)
    cursor = db.cursor()

    needs_commit = any(
        keyword in query.strip().lower() for keyword in mysql_commit_keywords
    )
    result = execute_mysql_query(cursor, query, commit=needs_commit)

    # Caller will close the connection after finishing all operations.
    return result, db
