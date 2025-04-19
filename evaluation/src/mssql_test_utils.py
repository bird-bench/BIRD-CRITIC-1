from datetime import date, datetime
from mssql_utils import execute_queries, perform_query_on_sqlserver_databases
import xml.etree.ElementTree as ET


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


def parse_estimated_subtree_cost(plan_xml_str):
    """
    Parses the SHOWPLAN_XML string to extract the highest cost.
    """
    if not plan_xml_str or not plan_xml_str.strip():
        return 0.0

    try:
        root = ET.fromstring(plan_xml_str)
    except ET.ParseError:
        return 0.0

    max_cost = 0.0
    for relop in root.findall(".//{*}RelOp"):
        cost_str = relop.get("EstimatedTotalSubtreeCost") or relop.get(
            "EstimatedSubtreeCost"
        )
        if cost_str:
            try:
                cost_val = float(cost_str)
                if cost_val > max_cost:
                    max_cost = cost_val
            except ValueError:
                pass

    for stmt in root.findall(".//{*}StmtSimple"):
        cost_str = stmt.get("StatementSubTreeCost") or stmt.get("EstimatedSubtreeCost")
        if cost_str:
            try:
                cost_val = float(cost_str)
                if cost_val > max_cost:
                    max_cost = cost_val
            except ValueError:
                pass

    return max_cost


def performance_compare_by_qep(old_sqls, sol_sqls, db_name, conn):
    """
    Compare two SQL execution plans by estimated cost.
    """
    if not old_sqls or not sol_sqls:
        print("Either old_sqls or sol_sqls is empty. Returning 0.")
        return 0

    def measure_sqls_cost(sql_list):
        total_cost = 0.0
        perform_query_on_sqlserver_databases("SET SHOWPLAN_XML ON;", db_name, conn)
        for sql in sql_list:
            try:
                rows, _ = perform_query_on_sqlserver_databases(
                    sql, db_name, conn, False
                )
                if rows and len(rows) > 0 and rows[0][0]:
                    plan_xml = rows[0][0]
                    cost = parse_estimated_subtree_cost(plan_xml)
                    total_cost += cost
            except Exception as e:
                print(f"[measure_sqls_cost] Error on sql: {sql}, {e}")
        perform_query_on_sqlserver_databases("SET SHOWPLAN_XML OFF;", db_name, conn)
        return total_cost

    old_total_cost = measure_sqls_cost(old_sqls)
    sol_total_cost = measure_sqls_cost(sol_sqls)

    print(
        f"[performance_compare_by_qep] Compare old({old_total_cost}) vs. sol({sol_total_cost})"
    )
    return 1 if sol_total_cost < old_total_cost else 0


def ex_base(pred_sqls, sol_sqls, db_name, conn):
    """
    Compare the result sets of two SQL statements for an exact match.
    """
    if not pred_sqls or not sol_sqls:
        return 0

    def calculate_ex(predicted_res, ground_truth_res):
        return 1 if set(predicted_res) == set(ground_truth_res) else 0

    predicted_res, pred_execution_error, pred_timeout_error = execute_queries(
        pred_sqls, db_name, conn, None, "", True
    )
    ground_truth_res, gt_execution_error, gt_timeout_error = execute_queries(
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
        print(f"SQLs (argument pred_sqls) has execution error {gt_execution_error}")
        print(f"SQLs (argument pred_sqls) has timeout error {gt_timeout_error}")
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

    predicted_res, pred_execution_error, pred_timeout_error = execute_queries(
        pred_sqls, db_name, conn, None, "", True, True
    )
    ground_truth_res, gt_execution_error, gt_timeout_error = execute_queries(
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
