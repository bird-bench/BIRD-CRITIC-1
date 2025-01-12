import json


def assistant_prompt(json_data):
    query = json_data["query"]
    error_sql = json_data["error_sql"]
    error_sql_str = ""
    for sql in error_sql:
        error_sql_str += f"```sql\n{sql}\n```\n"
    db_name = json_data["selected_database"]
    with open("./dev_schema.json", "r") as f:
        schema = json.load(f)
    table_schema = schema[db_name]
    return f"""You are a SQL assistant. Your task is to understand user issue and correct their problematic SQL given the database schema. Please wrap your corrected SQL with ```sql\n [Your Fixed SQL] \n``` tags in your response.
# Database Schema:
{table_schema}
# User issue:
{query}
# Problematic SQL:
{error_sql_str}
# Corrected SQL:
"""


# Corrected SQL:
# ```sql
