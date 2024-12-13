#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL Database Setup and Query Execution

This module provides functions to connect to a PostgreSQL database,
execute queries, and manage database connections.
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

postgresql_commit_keywords = (
    "insert",
    "update",
    "delete",
    "create",
    "drop",
    "alter",
    "truncate",
    "comment",
    "copy",
    "grant",
    "revoke",
    "analyze",
    "vacuum",
    "cluster",
    "reindex",
    "declare",
    "execute",
    "explain analyze",
    "listen",
    "notify",
    "load",
    "lock",
    "prepare transaction",
    "commit prepared",
    "rollback prepared",
    "reassign owned",
    "refresh materialized view",
    "security label",
)


def connect_postgresql(db_name):
    """
    Establish a connection to the PostgreSQL database.

    Args:
        db_name (str): The name of the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: A connection object to the database.
    """
    return psycopg2.connect(
        dbname=db_name,
        user="root",  # PostgreSQL user in Docker setup
        host="bird_critic_postgresql",  # Docker host (assuming exposed on bird_critic_postgresql)
        password="123123",  # Password used in Docker PostgreSQL setup
        port="5432",  # Port exposed by Docker in docker-compose.yml
    )


def execute_postgresql_query(cursor, query, commit=False):
    """
    Execute a PostgreSQL query and optionally commit changes.

    Args:
        cursor (psycopg2.extensions.cursor): The database cursor.
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
    except psycopg2.ProgrammingError:
        # No results to fetch (e.g., for INSERT queries)
        result = None
    return result


def perform_query_on_postgresql_databases(query, db_name):
    """
    Perform a query on PostgreSQL databases, managing connection and cursor.

    Args:
        query (str): The SQL query to execute.
        db_name (str): The name of the PostgreSQL database.

    Returns:
        list or None: The query results if any, None for write operations.
    """
    db = connect_postgresql(db_name)
    # cursor = db.cursor(cursor_factory=DictCursor)  # Use DictCursor for dictionary results if you prefer
    cursor = db.cursor()
    try:
        flag = False
        # check if the commit keyword in the query
        for keyword in postgresql_commit_keywords:
            if keyword in query.strip().lower():
                flag = True
                break
        if flag:
            result = execute_postgresql_query(cursor, query, commit=True)
        else:
            result = execute_postgresql_query(cursor, query)
    finally:
        cursor.close()
        db.close()
    return result
