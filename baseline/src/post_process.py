import argparse
import json
import re
import sys
from util import sql_response_extract


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
                sql_list = sql_response_extract(response)
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
