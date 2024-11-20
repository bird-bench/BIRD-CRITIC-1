#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Table Schema Generation for MySQL and PostgreSQL

This module provides functions to generate schema prompts and example data
for MySQL and PostgreSQL databases.
"""

import re
import pandas as pd
from mysql_setup import connect_mysql
from postgresql_setup import connect_postgresql

# Database to table mapping
db_table_map = {
    "debit_card_specializing": [
        "customers",
        "gasstations",
        "products",
        "transactions_1k",
        "yearmonth",
    ],
    "student_club": [
        "major",
        "member",
        "attendance",
        "budget",
        "event",
        "expense",
        "income",
        "zip_code",
    ],
    "thrombosis_prediction": ["Patient", "Examination", "Laboratory"],
    "european_football_2": [
        "League",
        "Match",
        "Player",
        "Player_Attributes",
        "Team",
        "Team_Attributes",
    ],
    "formula_1": [
        "circuits",
        "seasons",
        "races",
        "constructors",
        "constructorResults",
        "constructorStandings",
        "drivers",
        "driverStandings",
        "lapTimes",
        "pitStops",
        "qualifying",
        "status",
        "results",
    ],
    "superhero": [
        "alignment",
        "attribute",
        "colour",
        "gender",
        "publisher",
        "race",
        "superpower",
        "superhero",
        "hero_attribute",
        "hero_power",
    ],
    "codebase_community": [
        "posts",
        "users",
        "badges",
        "comments",
        "postHistory",
        "postLinks",
        "tags",
        "votes",
    ],
    "card_games": [
        "cards",
        "foreign_data",
        "legalities",
        "rulings",
        "set_translations",
        "sets",
    ],
    "toxicology": ["molecule", "atom", "bond", "connected"],
    "california_schools": ["satscores", "frpm", "schools"],
    "financial": [
        "district",
        "account",
        "client",
        "disp",
        "card",
        "loan",
        "order",
        "trans",
    ],
}


def format_mysql_create_table(raw_schema):
    """
    Format MySQL CREATE TABLE statement by removing ENGINE clause.

    Args:
        raw_schema (tuple): Raw schema information from MySQL.

    Returns:
        str: Formatted CREATE TABLE statement.
    """
    sql = raw_schema[0][1]
    cleaned_sql = re.sub(r"\s*ENGINE.*$", "", sql, flags=re.MULTILINE)
    return cleaned_sql


def generate_schema_prompt_mysql(db_name, table_name, num_rows=3):
    """
    Generate schema prompt for MySQL tables including example data.

    Args:
        db_name (str): Name of the database.
        table_name (str): Name of the table.
        num_rows (int, optional): Number of example rows to fetch. Defaults to 3.

    Returns:
        str: Formatted schema and example data for specified tables.
    """
    db = connect_mysql(db_name)
    cursor = db.cursor()
    query = f"""
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_TYPE = 'BASE TABLE';
    """
    cursor.execute(query)
    tables = cursor.fetchall()
    schemas = {}
    for table in tables:
        curr_table_name = table[0]
        if curr_table_name not in db_table_map[db_name]:
            continue
        if table_name and curr_table_name != table_name:
            continue

        # Get table schema
        cursor.execute(f"SHOW CREATE TABLE `{curr_table_name}`;")
        raw_schema = cursor.fetchall()
        # print("raw_schema", raw_schema)
        pretty_schema = format_mysql_create_table(raw_schema)
        # print("pretty_schema", pretty_schema)
        # Get example rows
        cursor.execute(f"SELECT * FROM `{curr_table_name}` LIMIT {num_rows};")
        example_rows = cursor.fetchall()

        # Format example rows
        example_data = "Example data:\n"
        if example_rows:
            # convert the tuple to dictionary
            example_rows = [
                dict(zip([column[0] for column in cursor.description], row))
                for row in example_rows
            ]
            # using pandas to format the data
            example_rows = pd.DataFrame(example_rows)
            example_data += example_rows.to_string(index=False)
        else:
            example_data += "No data available in this table.\n"

        schemas[curr_table_name] = f"{pretty_schema}\n\n{example_data}"

    schema_prompt = "\n\n".join(schemas.values())
    db.close()
    return schema_prompt


def format_postgresql_create_table(
    table_name, columns_info, primary_keys, foreign_keys
):
    """
    Format PostgreSQL CREATE TABLE statement.

    Args:
        table_name (str): Name of the table.
        columns_info (list): List of column information.
        primary_keys (list): List of primary key columns.
        foreign_keys (list): List of foreign key information.

    Returns:
        str: Formatted CREATE TABLE statement.
    """
    lines = [f"CREATE TABLE {table_name} ("]
    for i, column in enumerate(columns_info):
        column_name, data_type, is_nullable, column_default = column
        null_status = "NULL" if is_nullable == "YES" else "NOT NULL"
        default = f"DEFAULT {column_default}" if column_default else ""
        column_line = f"    {column_name} {data_type} {null_status} {default}".strip()
        if i < len(columns_info) - 1 or primary_keys or foreign_keys:
            column_line += ","
        lines.append(column_line)

    if primary_keys:
        pk_line = f"    PRIMARY KEY ({', '.join(primary_keys)})"
        if foreign_keys:
            pk_line += ","
        lines.append(pk_line)

    for fk in foreign_keys:
        fk_column, ref_table, ref_column = fk
        fk_line = f"    FOREIGN KEY ({fk_column}) REFERENCES {ref_table}({ref_column})"
        if fk != foreign_keys[-1]:
            fk_line += ","
        lines.append(fk_line)

    lines.append(");")
    return "\n".join(lines)


def generate_schema_prompt_postgresql(db_name, table_name, num_rows=3):
    """
    Generate schema prompt for PostgreSQL tables including example data.

    Args:
        db_name (str): Name of the database.
        table_name (str, optional): Name of the table.
        num_rows (int, optional): Number of example rows to fetch. Defaults to 3.

    Returns:
        str: Formatted schema and example data for specified tables.
    """
    db = connect_postgresql(db_name)
    cursor = db.cursor()
    tables = [table for table in db_table_map[db_name]]
    schemas = {}

    for table in tables:
        if table_name and table_name != table:
            continue

        # Get column information
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """,
            (table,),
        )
        columns_info = cursor.fetchall()

        # Get primary key information
        cursor.execute(
            """
            SELECT a.attname
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = %s::regclass AND i.indisprimary;
        """,
            (table,),
        )
        primary_keys = [row[0] for row in cursor.fetchall()]

        # Get foreign key information
        cursor.execute(
            """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = %s;
        """,
            (table,),
        )
        foreign_keys = cursor.fetchall()

        pretty_schema = format_postgresql_create_table(
            table, columns_info, primary_keys, foreign_keys
        )

        # Get example rows
        cursor.execute(f"SELECT * FROM {table} LIMIT {num_rows};")
        example_rows = cursor.fetchall()

        # Format example rows
        example_data = "Example data:\n"
        if example_rows:
            # Convert the tuple to dictionary
            example_rows = [
                dict(zip([column[0] for column in cursor.description], row))
                for row in example_rows
            ]
            # Using pandas to format the data
            example_rows = pd.DataFrame(example_rows)
            example_data += example_rows.to_string(index=False)
        else:
            example_data += "No data available in this table.\n"

        schemas[table] = f"{pretty_schema}\n\n{example_data}"

    schema_prompt = "\n\n".join(schemas.values())
    cursor.close()
    db.close()
    return schema_prompt


def generate_schema_prompt(sql_dialect, db_name, table_name):
    """
    Generate schema prompt based on SQL dialect.

    Args:
        sql_dialect (str): SQL dialect ('MySQL' or 'PostgreSQL').
        db_name (str, optional): Name of the database.
        table_name (str, optional): Name of the table.

    Returns:
        str: Formatted schema and example data.

    Raises:
        ValueError: If an unsupported SQL dialect is provided.
    """
    if sql_dialect == "MySQL":
        return generate_schema_prompt_mysql(db_name, table_name)
    elif sql_dialect == "PostgreSQL":
        return generate_schema_prompt_postgresql(db_name, table_name)
    else:
        raise ValueError("Unsupported SQL dialect: {}".format(sql_dialect))
