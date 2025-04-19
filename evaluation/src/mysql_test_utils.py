from datetime import date, datetime
import json
from mysql_utils import perform_query_on_mysql_databases, execute_queries


def preprocess_results(results):
    """
    Preprocess SQL query results, converting datetime objects to "yyyy-mm-dd" string format.

    Args:
        results (list of tuples): Result set from SQL query.

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
    Preprocess SQL query results, converting date/datetime objects in each dictionary
    to "yyyy-mm-dd" string format.

    Args:
        results (list of dict): SQL query result set, where each element is a dictionary.

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


def check_sql_function_usage(sqls, required_keywords):
    """
    Check if the specified keywords or functions are used in pred_sqls (in list form),
    focusing only on whether all appear. Returns 1 if all appear; otherwise returns 0.

    Args:
        sqls (list[str]): List of SQL predictions generated
        required_keywords (list[str]): List of keywords or functions that must appear

    Returns:
        int: 1 indicates all keywords are present, 0 indicates at least one required keyword is missing
    """
    # If pred_sqls is an empty list or None, return 0 directly
    if not sqls:
        return 0

    # Concatenate all SQLs together and convert to lowercase for case-insensitive comparison
    combined_sql = " ".join(sql.lower() for sql in sqls)

    # Check if all required keywords appear in combined_sql
    for kw in required_keywords:
        if kw.lower() not in combined_sql:
            return 0

    return 1


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
        # Rebuild the query string without 'DISTINCT'
        cleaned_query = " ".join(filtered_tokens)
        cleaned_queries.append(cleaned_query)

    return cleaned_queries


def ex_base(pred_sqls, sol_sqls, db_name, conn):
    """
    Example comparison function—compares sets of rows from pred_sqls vs. sol_sqls on MySQL.
    """
    if not pred_sqls or not sol_sqls:
        return 0

    def calculate_ex(predicted_res, ground_truth_res):
        return 1 if set(predicted_res) == set(ground_truth_res) else 0

    predicted_res, _, _, _ = execute_queries(pred_sqls, db_name, conn, None, "", True)
    ground_truth_res, _, _, _ = execute_queries(sol_sqls, db_name, conn, None, "", True)
    if not predicted_res or not ground_truth_res:
        return 0

    predicted_res = preprocess_results(predicted_res)
    ground_truth_res = preprocess_results(ground_truth_res)
    return calculate_ex(predicted_res, ground_truth_res)


def performance_compare_by_qep(old_sqls, sol_sqls, db_name, conn):
    """
    MySQL approach: Use EXPLAIN FORMAT=JSON, parse some cost or row estimates, compare sums.
    """

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
                print(f"[measure_sqls_cost] Skip EXPLAIN for non-DML: {sql}")
                # just run it
                try:
                    perform_query_on_mysql_databases(sql, db_name, conn)
                except Exception as exc:
                    print(f"[measure_sqls_cost] Error executing '{sql}': {exc}")
                continue

            explain_sql = f"EXPLAIN FORMAT=JSON {sql}"
            try:
                rows, _ = perform_query_on_mysql_databases(
                    explain_sql, db_name, conn=conn
                )
                if not rows:
                    print(f"[measure_sqls_cost] No EXPLAIN rows returned for {sql}")
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
                    except Exception as e:
                        print(f"Failed to parse EXPLAIN JSON: {e}")
            except Exception as e:
                print(f"[measure_sqls_cost] Unexpected error on SQL '{sql}': {e}")
        return total_cost

    # Measure cost for old_sqls
    old_total_cost = measure_sqls_cost(old_sqls)
    # Measure cost for sol_sqls
    sol_total_cost = measure_sqls_cost(sol_sqls)

    return 1 if sol_total_cost < old_total_cost else 0
