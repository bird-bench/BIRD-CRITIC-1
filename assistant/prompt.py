db_schema_w_column_meaning = "./schema_with_meanings.json"
db_schema_wo_column_meaning = "./schema_without_meanings.json"
import json


def load_schema(type):
    if type == "with_meaning":
        with open(db_schema_w_column_meaning, "r") as f:
            data = json.load(f)
    else:
        with open(db_schema_wo_column_meaning, "r") as f:
            data = json.load(f)

    return data


def assistant_prompt(json_data):
    query = json_data["query"]
    error_sql = "\n".join(json_data["error_sql"])
    db_name = json_data["selected_database"]
    schema = load_schema("wo_meaning")
    table_schema = schema[db_name]
    return f"""You are a SQL assistant to help a user with their SQL queries.
# Database Schema:
{table_schema}
# User Query:
{query}
# Error SQL:
{error_sql}
Your task is to solve the user's query referring to the database schema provided above. Please wrap your solution SQLs with ```sql\n [Your Fixed SQL] \n``` tags in your response."""
