generate_tests_prompt_template = """
We are currently solving the following issue within our repository. Here is the issue text:
--- BEGIN ISSUE ---
{problem_statement}
--- END ISSUE ---

Please generate a complete test that can be used to reproduce the issue.

The complete test should contain the following:
1. Code to reproduce the issue described in the issue text
2. Print "Issue reproduced" if the outcome indicates that the issue is reproduced: either an exception is raised or the outcome is incorrect
3. Print "Issue resolved" if the outcome indicates that the issue has been successfully resolved
5. The name of the test function must be test_issue with two parameter sql and db

Here is an example:
```python
def test_issue(sql, db) -> None:
    try:
        result, conn = perform_query_on_mysql_databases(sql, db)
    except:
        print("Issue reproduced")
        return
    try:
        assert result[0] == ("HR",)
        assert result[1] == ("Finance",)
        print("Issue resolved")
    except AssertionError:
        print("Issue reproduced")
        return
```

Please ensure the generated test reflects the issue described in the provided issue text.
The generated test should be able to be used to both reproduce the issue as well as to verify the issue has been fixed.
Wrap the complete test in ```python...```.
"""

# perform_query_on_mysql_databases(query, db_name) is used to query the database.
# This function accept two arguments:
# query (str): The SQL query to execute.
# db_name (str): The name of the database to query.
# And returns a tuple: (result, conn)
# result: The query results if any, None for write operations.
# conn:   The open database connection. The caller must close it.

# db = "employees"

# # This will print "Other issues"
# sql = "SELECT department FROM employees WHERE'"
# test_issue(sql,db)

# # This will print "Issue reproduced"
# sql = "SELECT department FROM employees WHERE employee.name LIKE 'Jack%'"
# test_issue(sql,db)


# # This will print "Issue resolved"
# sql = "SELECT department FROM employees WHERE employee.name LIKE 'John%'"
# test_issue(sql,db)
