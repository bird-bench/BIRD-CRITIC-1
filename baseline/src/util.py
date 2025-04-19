import os
import json
import re


def load_jsonl(file_path):
    data = []
    with open(file_path, "r") as file:
        for line in file:
            data.append(json.loads(line))
    return data


def new_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def write_response(results, data_list, output_path):
    """Write responses to an output file in JSONL format."""
    if output_path:
        new_directory(os.path.dirname(output_path))
        with open(output_path, "w") as f:
            for i, data in enumerate(data_list):
                data["response"] = results[i]
                f.write(json.dumps(data) + "\n")


def sql_response_extract(response_string):
    """
    Extract all SQL code blocks wrapped with ```sql and ``` from the response string.
    Returns a list of SQL statements.
    """
    sql_pattern = re.compile(r"```[ \t]*sql\s*([\s\S]*?)```", re.IGNORECASE | re.DOTALL)

    sql_statements = sql_pattern.findall(response_string)
    sql_statements = [stmt.strip() for stmt in sql_statements]

    return sql_statements
