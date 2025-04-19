from datetime import date, datetime
from postgresql_utils import (
    perform_query_on_postgresql_databases,
    execute_queries,
)
import psycopg2
import json


def preprocess_results(results):
    """
    Preprocess SQL query results by converting datetime objects into "yyyy-mm-dd" string format.

    Args:
        results (list of tuples): The result set from the SQL query.

    Returns:
        list of tuples: The processed result set where all datetime objects are converted to strings.
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


def remove_distinct(sql_list):
    """
    Remove all occurrences of the DISTINCT keyword (in any case form)
    from a single list of SQL query strings. This is a brute-force
    approach without using regular expressions.

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
        cleaned_query = " ".join(filtered_tokens)
        cleaned_queries.append(cleaned_query)

    return cleaned_queries


def check_sql_function_usage(sqls, required_keywords):
    """
    Check if the list of predicted SQL queries uses all of the specified keywords or functions.
    Returns 1 if all required keywords appear; otherwise returns 0.

    Args:
        sqls (list[str]): The list of predicted SQL queries.
        required_keywords (list[str]): The list of required keywords or functions.

    Returns:
        int: 1 if all required keywords appear, 0 if at least one is missing.
    """
    # Return 0 immediately if sqls is empty or None
    if not sqls:
        return 0

    # Combine all SQL queries into one string and convert to lowercase
    combined_sql = " ".join(sql.lower() for sql in sqls)

    # Check if all required keywords appear in combined_sql
    for kw in required_keywords:
        if kw.lower() not in combined_sql:
            return 0

    return 1


def ex_base(pred_sqls, sol_sqls, db_name, conn):
    """
    Execute predicted SQL list and ground truth SQL list, and compare if the results are identical.
    Returns 1 if identical, otherwise 0.
    """
    # If either list is empty, return 0
    if not pred_sqls or not sol_sqls:
        return 0

    def calculate_ex(predicted_res, ground_truth_res):
        # Compare results as sets to ignore order and duplicates
        return 1 if set(predicted_res) == set(ground_truth_res) else 0

    # Execute predicted SQL list
    predicted_res, pred_execution_error, pred_timeout_error = execute_queries(
        pred_sqls, db_name, conn, None, ""
    )

    # Execute ground truth SQL list
    ground_truth_res, gt_execution_error, gt_timeout_error = execute_queries(
        sol_sqls, db_name, conn, None, ""
    )

    # If any execution or timeout error occurs, return 0
    if (
        gt_execution_error
        or gt_timeout_error
        or pred_execution_error
        or pred_timeout_error
    ):
        return 0

    # If results are None or empty, return 0
    if not predicted_res or not ground_truth_res:
        return 0

    predicted_res = preprocess_results(predicted_res)
    ground_truth_res = preprocess_results(ground_truth_res)

    return calculate_ex(predicted_res, ground_truth_res)


def performance_compare_by_qep(old_sqls, sol_sqls, db_name, conn):
    """
    Compare total plan cost of old_sqls vs. sol_sqls in one connection,
    by using transactions + ROLLBACK to ensure each group sees the same initial state.

    Returns 1 if sol_sqls total plan cost is lower, otherwise 0.

    Notes:
      - If old_sqls/sol_sqls contain schema changes or data modifications,
        we rely on transaction rollback to discard those changes before measuring the other side.
      - EXPLAIN does not execute the query; it only returns the plan and cost estimate.
      - This approach ensures both sets see the same starting state for cost comparison.
    """

    if not old_sqls or not sol_sqls:
        print("Either old_sqls or sol_sqls is empty. Returning 0.")
        return 0
    print(f"Old SQLs are {old_sqls}")
    print(f"New SQLs are {sol_sqls}")

    def measure_sqls_cost(sql_list):
        """
        Measure the sum of 'Total Cost' for each DML statement in sql_list
        via EXPLAIN (FORMAT JSON). Non-DML statements are just executed, but not included in the total cost.
        """
        total_cost = 0.0
        for sql in sql_list:
            upper_sql = sql.strip().upper()
            # We only measure DML cost for SELECT/INSERT/UPDATE/DELETE
            if not (
                upper_sql.startswith("SELECT")
                or upper_sql.startswith("INSERT")
                or upper_sql.startswith("UPDATE")
                or upper_sql.startswith("DELETE")
                or upper_sql.startswith("WITH")
            ):
                print(f"[measure_sqls_cost] Skip EXPLAIN for non-DML: {sql}")
                try:
                    perform_query_on_postgresql_databases(sql, db_name, conn=conn)
                except Exception as exc:
                    print(f"[measure_sqls_cost] Error executing non-DML '{sql}': {exc}")
                continue

            explain_sql = f"EXPLAIN (FORMAT JSON) {sql}"
            try:
                result_rows, _ = perform_query_on_postgresql_databases(
                    explain_sql, db_name, conn=conn
                )
                if not result_rows:
                    print(f"[measure_sqls_cost] No result returned for EXPLAIN: {sql}")
                    continue

                explain_json = result_rows[0][0]
                if isinstance(explain_json, str):
                    explain_json = json.loads(explain_json)

                if isinstance(explain_json, list) and len(explain_json) > 0:
                    plan_info = explain_json[0].get("Plan", {})
                    total_cost_part = plan_info.get("Total Cost", 0.0)
                else:
                    print(
                        f"[measure_sqls_cost] Unexpected EXPLAIN JSON format for {sql}, skip cost."
                    )
                    total_cost_part = 0.0

                total_cost += float(total_cost_part)

            except psycopg2.Error as e:
                print(f"[measure_sqls_cost] psycopg2 Error on SQL '{sql}': {e}")
            except Exception as e:
                print(f"[measure_sqls_cost] Unexpected error on SQL '{sql}': {e}")

        return total_cost

    # Measure cost for old_sqls
    try:
        perform_query_on_postgresql_databases("BEGIN", db_name, conn=conn)
        old_total_cost = measure_sqls_cost(old_sqls)
        print(f"Old SQLs total plan cost: {old_total_cost}")
    finally:
        perform_query_on_postgresql_databases("ROLLBACK", db_name, conn=conn)

    # Measure cost for sol_sqls
    try:
        perform_query_on_postgresql_databases("BEGIN", db_name, conn=conn)
        sol_total_cost = measure_sqls_cost(sol_sqls)
        print(f"Solution SQLs total plan cost: {sol_total_cost}")
    finally:
        perform_query_on_postgresql_databases("ROLLBACK", db_name, conn=conn)

    # Compare final costs
    print(
        f"[performance_compare_by_qep] Compare old({old_total_cost}) vs. sol({sol_total_cost})"
    )
    return 1 if sol_total_cost < old_total_cost else 0
