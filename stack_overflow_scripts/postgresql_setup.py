#!/usr/bin/env python3
# -*- coding: utf-8 -*-

TABLE_ORDER = [
    "hero_power",
    "votes",
    "disp",
    "foreign_data",
    "attendance",
    "loan",
    "myperms",
    "postlinks",
    "superpower",
    "match",
    "molecule",
    "qualifying",
    "badges",
    "yearmonth",
    "connected",
    "event",
    "my_table",
    "set_translations",
    "rulings",
    "expense",
    "card",
    "laptimes",
    "posthistory",
    "cards",
    "results",
    "hero_attribute",
    "legalities",
    "tags",
    "player_attributes",
    "laboratory",
    "member",
    "status",
    "products",
    "proccd",
    "trans",
    "zip_code",
    "seasons",
    "schools",
    "team_attributes",
    "sets",
    "pitstops",
    "satscores",
    "examination",
    "transactions_1k",
    "order",
    "patient",
    "district",
    "comments",
    "superhero",
    "frpm",
    "income",
    "gasstations",
    "constructorstandings",
    "constructorresults",
    "league",
    "driverstandings",
    "users",
    "posts",
    "client",
    "customers",
    "atom",
    "bond",
    "budget",
    "races",
    "attribute",
    "player",
    "major",
    "team",
    "account",
    "race",
    "publisher",
    "gender",
    "alignment",
    "colour",
    "constructors",
    "country",
    "drivers",
    "circuits",
]
POST_DATABASE_MAPPING = {
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
        "driverstandings",
        "races",
        "constructors",
        "constructorresults",
        "laptimes",
        "qualifying",
        "pitstops",
        "seasons",
        "constructorstandings",
        "results",
    ],
    "california_schools": [
        "schools",
        "satscores",
        "frpm",
    ],
    "card_games": [
        "legalities",
        "cards",
        "rulings",
        "set_translations",
        "sets",
        "foreign_data",
    ],
    "european_football_2": [
        "team_attributes",
        "player",
        "match",
        "league",
        "country",
        "player_attributes",
        "team",
    ],
    "thrombosis_prediction": [
        "laboratory",
        "patient",
        "examination",
    ],
    "toxicology": [
        "bond",
        "molecule",
        "atom",
        "connected",
    ],
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
        "postlinks",
        "posthistory",
        "badges",
        "posts",
        "users",
        "tags",
        "votes",
        "comments",
    ],
}
"""
PostgreSQL Query Execution with Connection Pool
Allows reusing the same connection across multiple perform_query calls.
"""

import psycopg2
from psycopg2.pool import SimpleConnectionPool

POSTGRESQL_COMMIT_KEYWORDS = (
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
    Executes the given query on the specified database.

    1. If conn is None, we fetch a connection from the pool.
    2. If conn is provided, we reuse that connection.
    3. We automatically commit if it's a write operation.
    4. We return (result, conn), so the caller can reuse 'conn' for subsequent queries.

    Args:
        query (str): The SQL statement to execute.
        db_name (str): The target database name.
        conn (psycopg2.extensions.connection, optional): An existing connection.
          If None, we'll get a new one from the pool.

    Returns:
        (result, connection):
            - result: The rows if it's a SELECT-like query, else None
            - connection: The connection used (either newly acquired or reused)
    """
    MAX_ROWS = 10000  # 固定的最大行数限制

    pool = _get_or_init_pool(db_name)
    need_to_put_back = False  # 标记是否需要将连接放回池中

    if conn is None:
        conn = pool.getconn()
        need_to_put_back = True

    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '60s';")

    try:
        cursor.execute(query)
        lower_q = query.strip().lower()
        if any(kw in lower_q for kw in POSTGRESQL_COMMIT_KEYWORDS):
            conn.commit()

        if lower_q.startswith("select") or lower_q.startswith("with"):
            # fetchmany is used to limit the number of rows fetched
            rows = cursor.fetchmany(MAX_ROWS + 1)
            if len(rows) > MAX_ROWS:
                rows = rows[:MAX_ROWS]
            result = rows
        else:
            try:
                result = cursor.fetchall()
            except psycopg2.ProgrammingError:
                result = None

        return result, conn

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        # IMPORTANT: Do NOT putconn(conn) here,
        # because the user wants to keep using the SAME connection.
        # We'll only putconn if we see the user never wants to reuse it.
        if need_to_put_back:
            # If you truly only want one query and done, you could do:
            # pool.putconn(conn)
            pass


def close_postgresql_connection(db_name, conn):
    """
    After the user is finished using this connection (e.g., after multiple queries),
    they call this function to release it back to the pool.
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
    Closes the connection pool for the specified database.

    Args:
        db_name (str): The database name for which to close the pool.
    """
    if db_name in _postgresql_pools:
        pool = _postgresql_pools.pop(db_name)
        pool.closeall()


def get_conn(db_name):
    """
    Returns a psycopg2 connection for the given db_name from the connection pool.
    The caller is responsible for releasing it by calling close_postgresql_connection(db_name, conn).
    """
    pool = _get_or_init_pool(db_name)
    conn = pool.getconn()
    return conn


# =====================
# Example usage
# =====================
# Example 1: Acquire a connection once, use it for multiple queries

# Acquire first connection from the pool (conn1 is None, so we borrow it)
# rows, conn1 = perform_query_on_postgresql_databases("SELECT version();", "debit_card_specializing")
# print("PostgreSQL version:", rows)

# # Use the same connection again
# rows, conn1 = perform_query_on_postgresql_databases("SELECT * FROM yearmonth LIMIT 5", "debit_card_specializing",conn1)
# print("Result:", rows)

# # Once done with conn1, user calls:
# close_postgresql_connection("debit_card_specializing", conn1)

# # Finally, close all pools
# close_all_postgresql_pools()
