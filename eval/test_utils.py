from datetime import date, datetime


def preprocess_results(results):
    """
    Preprocess SQL query results by converting datetime objects to the "yyyy-mm-dd" string format.

    Args:
        results (list of tuples): The result set of an SQL query.

    Returns:
        list of tuples: The processed result set, with all datetime objects converted to strings.
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
    from a list of SQL query strings. This is a brute-force
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


def check_sql_function_usage(sqls, required_keywords):
    """
    Check whether the list of predicted SQL queries (`sqls`) contains all of the specified
    required keywords or functions (case-insensitive). If all required keywords appear,
    return 1; otherwise, return 0.

    Parameters:
        sqls (list[str]): The predicted SQL queries.
        required_keywords (list[str]): The list of required keywords or functions.

    Returns:
        int: 1 if all required keywords appear, 0 if at least one required keyword is missing.
    """
    # If sqls is an empty list or None, return 0 directly
    if not sqls:
        return 0

    # Concatenate all SQL queries into one string, and convert to lowercase for case-insensitive comparison
    combined_sql = " ".join(sql.lower() for sql in sqls)

    # Check if all required keywords appear in combined_sql
    for kw in required_keywords:
        if kw.lower() not in combined_sql:
            return 0

    return 1
