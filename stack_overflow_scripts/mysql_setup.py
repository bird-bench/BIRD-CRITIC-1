#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL Database Setup and Query Execution

This module provides functions to connect to a MySQL database,
execute queries, and manage database connections.
"""

import pymysql

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
    Establish a connection to the MySQL database.

    Args:
        db_name (str): The name of the database to connect to.

    Returns:
        pymysql.connections.Connection: A connection object to the database.
    """
    # Connect to the Docker-hosted MySQL database
    db = pymysql.connect(
        host="bird_critic_mysql",  # Hostname for accessing MySQL service within Docker network
        user="root",
        password="123123",
        database=db_name,
        port=3306,
    )
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
    Perform a query on MySQL databases, managing connection and cursor.

    Args:
        query (str): The SQL query to execute.
        db_name (str): The name of the database to query.

    Returns:
        list or None: The query results if any, None for write operations.
    """
    db = connect_mysql(db_name)
    cursor = db.cursor()
    # cursor = db.cursor(pymysql.cursors.DictCursor)  # Use DictCursor for dictionary results if you prefer
    try:
        flag = False
        # check if the commit keyword in the query
        for keyword in mysql_commit_keywords:
            if keyword in query.strip().lower():
                flag = True
                break
        if flag:
            result = execute_mysql_query(cursor, query, commit=True)
        else:
            result = execute_mysql_query(cursor, query)
    finally:
        db.close()
    return result
