#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility functions for testing SQL queries in Oracle.
"""

from datetime import date, datetime
import json
import sys
import re
from datetime import datetime
from oracle_utils import perform_query_on_oracle_databases, execute_queries


def preprocess_results(results):
    """
    Preprocess SQL query results, converting datetime objects to "yyyy-mm-dd" string format.

    Args:
        results (list of tuples): SQL query result set.

    Returns:
        list of tuples: Processed result set with all datetime objects converted to strings.
    """
    processed = []
    for row in results:
        new_row = []
        for item in row:
            if isinstance(item, (date, datetime)):
                new_row.append(item.strftime("%Y-%m-%d"))
            else:
                new_row.append(item)
        processed.append(tuple(new_row))
    return processed


def preprocess_results_dict(results):
    """
    Preprocess SQL query results, converting datetime objects in dictionaries
    to "yyyy-mm-dd" string format.

    Args:
        results (list of dict): SQL query result set where each element is a dictionary.

    Returns:
        list of dict: Processed result set with all datetime/date objects converted to strings.
    """
    processed = []
    for row_dict in results:
        new_dict = {}
        for key, value in row_dict.items():
            if isinstance(value, (date, datetime)):
                new_dict[key] = value.strftime("%Y-%m-%d")
            else:
                new_dict[key] = value
        processed.append(new_dict)
    return processed


def remove_distinct(sql_list):
    """
    Remove all occurrences of the DISTINCT keyword (in any case form)
    from a single list of SQL query strings.

    Parameters:
    -----------
    sql_list : list of str
        A list of SQL queries (strings).

    Returns:
    --------
    list of str
        A new list of SQL queries with all 'DISTINCT' keywords removed.
    """
    cleaned_queries = []
    for query in sql_list:
        tokens = query.split()
        filtered_tokens = []
        for token in tokens:
            # Check if this token is 'distinct' (case-insensitive)
            if token.lower() != "distinct":
                filtered_tokens.append(token)
        # Rebuild the query string without 'DISTINCT'
        cleaned_query = " ".join(filtered_tokens)
        cleaned_queries.append(cleaned_query)

    return cleaned_queries


def check_sql_function_usage(sqls, required_keywords):
    """
    Check whether the predefined SQL statements (as a list) use all the specified
    keywords or functions. If all appear, return 1; otherwise, return 0.

    Args:
        sqls (list[str]): List of predicted SQL statements
        required_keywords (list[str]): List of required keywords or functions

    Returns:
        int: 1 if all required keywords appear, 0 if at least one required keyword is missing
    """
    # If sqls is an empty list or None, return 0
    if not sqls:
        return 0

    # Join all SQL statements and convert to lowercase for case-insensitive comparison
    combined_sql = " ".join(sql.lower() for sql in sqls)

    # Check if all required keywords appear in combined_sql
    for kw in required_keywords:
        if kw.lower() not in combined_sql:
            return 0

    return 1


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


def ex_base(pred_sqls, sol_sqls, db_name, conn):
    """
    Compare the result sets of two SQL statements for an exact match.
    """
    if not pred_sqls or not sol_sqls:
        return 0

    def calculate_ex(predicted_res, ground_truth_res):
        return 1 if set(predicted_res) == set(ground_truth_res) else 0

    predicted_res, pred_execution_error, pred_timeout_error, _ = execute_queries(
        pred_sqls, db_name, conn, None, "", True
    )
    ground_truth_res, gt_execution_error, gt_timeout_error, _ = execute_queries(
        sol_sqls, db_name, conn, None, "", True
    )

    if (
        gt_execution_error
        or gt_timeout_error
        or pred_execution_error
        or pred_timeout_error
    ):
        print(f"SQLs (argument sol_sqls) has execution error {gt_execution_error}")
        print(f"SQLs (argument sol_sqls) has timeout error {gt_timeout_error}")
        print(f"SQLs (argument pred_sqls) has execution error {pred_execution_error}")
        print(f"SQLs (argument pred_sqls) has timeout error {pred_timeout_error}")
        return 0

    if not predicted_res or not ground_truth_res:
        return 0
    predicted_res = preprocess_results(predicted_res)
    ground_truth_res = preprocess_results(ground_truth_res)
    return calculate_ex(predicted_res, ground_truth_res)


def ex_base_dict(pred_sqls, sol_sqls, db_name, conn):
    """
    Compare the result sets of two SQL statements for an exact match (dictionary-based).
    """
    if not pred_sqls or not sol_sqls:
        return 0

    def calculate_ex(predicted_res, ground_truth_res):
        predicted_set = {tuple(sorted(d.items())) for d in predicted_res}
        ground_truth_set = {tuple(sorted(d.items())) for d in ground_truth_res}
        return 1 if predicted_set == ground_truth_set else 0

    predicted_res, pred_execution_error, pred_timeout_error, _ = execute_queries(
        pred_sqls, db_name, conn, None, "", True, True
    )
    ground_truth_res, gt_execution_error, gt_timeout_error, _ = execute_queries(
        sol_sqls, db_name, conn, None, "", True, True
    )

    if (
        gt_execution_error
        or gt_timeout_error
        or pred_execution_error
        or pred_timeout_error
    ):
        print(f"SQLs (argument sol_sqls) has execution error {gt_execution_error}")
        print(f"SQLs (argument sol_sqls) has timeout error {gt_timeout_error}")
        print(f"SQLs (argument pred_sqls) has execution error {pred_execution_error}")
        print(f"SQLs (argument pred_sqls) has timeout error {pred_timeout_error}")
        return 0

    if not predicted_res or not ground_truth_res:
        return 0

    predicted_res = preprocess_results_dict(predicted_res)
    ground_truth_res = preprocess_results_dict(ground_truth_res)
    return calculate_ex(predicted_res, ground_truth_res)


def performance_compare_by_execution_plan(old_sqls, sol_sqls, db_name, conn):
    """
    Compare two SQL execution plans by estimated cost for Oracle.
    """
    if not old_sqls or not sol_sqls:
        print("Either old_sqls or sol_sqls is empty. Returning 0.")
        return 0

    def measure_sqls_cost(sql_list):
        total_cost = 0.0
        for sql in sql_list:
            try:
                clear_plan_table_sql = "DELETE FROM plan_table"
                perform_query_on_oracle_databases(clear_plan_table_sql, db_name, conn)
                # Add EXPLAIN PLAN statement
                explain_sql = f"EXPLAIN PLAN FOR {sql}"
                perform_query_on_oracle_databases(explain_sql, db_name, conn)

                # Fetch the plan
                plan_query = """
                SELECT SUM(cost) as total_cost
                FROM plan_table
                WHERE operation = 'SELECT STATEMENT'
                """
                rows, _ = perform_query_on_oracle_databases(plan_query, db_name, conn)

                if rows and len(rows) > 0 and rows[0][0]:
                    cost = float(rows[0][0])
                    total_cost += cost
            except Exception as e:
                print(f"[measure_sqls_cost] Error on sql: {sql}, {e}")

        return total_cost

    old_total_cost = measure_sqls_cost(old_sqls)
    sol_total_cost = measure_sqls_cost(sol_sqls)

    print(
        f"[performance_compare_by_execution_plan] Compare old({old_total_cost}) vs. sol({sol_total_cost})"
    )
    return 1 if sol_total_cost < old_total_cost else 0
