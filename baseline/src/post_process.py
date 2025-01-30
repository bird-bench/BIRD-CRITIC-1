import argparse
import json
import re
import sys


def extract_sql_from_response(response_string):
    """
    Extract all SQL code blocks wrapped with ```sql and ``` from the response string.
    Returns a list of SQL statements.
    """
    sql_pattern = re.compile(
        r"```sql\s*(.*?)```",
        re.IGNORECASE | re.DOTALL,
    )
    # Find all matches
    sql_statements = sql_pattern.findall(response_string)
    # Strip whitespace from each statement
    sql_statements = [stmt.strip() for stmt in sql_statements]

    return sql_statements


def process_file(input_file, output_file):
    """
    Process a JSONL file to extract SQL statements from responses.
    """
    with open(input_file, "r", encoding="utf-8") as infile, open(
        output_file, "w", encoding="utf-8"
    ) as outfile:
        for line_number, line in enumerate(infile, 1):
            # Parse the line as JSON
            try:
                data = json.loads(line.strip())
                response = data.get("response", "")

                # Extract SQL statements
                sql_list = extract_sql_from_response(response)
                print(
                    f"Extracted {len(sql_list)} SQL statements from line {line_number}"
                )
                # Add the list to the data
                data["pred_sqls"] = sql_list

                # Write the updated data
                outfile.write(json.dumps(data, ensure_ascii=False) + "\n")
            except json.JSONDecodeError:
                print(
                    f"Skipping invalid JSON line {line_number}: {line.strip()}",
                    file=sys.stderr,
                )


def main():
    parser = argparse.ArgumentParser(
        description="Extract SQL statements from LLM responses."
    )
    parser.add_argument(
        "--input_path", type=str, required=True, help="Path to the input JSONL file."
    )
    parser.add_argument(
        "--output_path", type=str, required=True, help="Path to the output JSONL file."
    )
    args = parser.parse_args()

    process_file(args.input_path, args.output_path)


if __name__ == "__main__":
    main()
