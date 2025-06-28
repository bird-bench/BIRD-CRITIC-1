import json
import os, sys
import argparse
from tqdm import tqdm

from util import load_jsonl, new_directory
from baseline_prompt import baseline_v1, baseline_v2


def write_prompts(prompts, data_list, prompt_path, limit):
    new_directory(os.path.dirname(prompt_path))
    with open(prompt_path, "w") as f:
        for i in range(limit):
            instance = data_list[i].copy()
            instance["prompt"] = prompts[i]
            f.write(json.dumps(instance, ensure_ascii=False) + "\n")


def generate_prompt(data, prompt_type, schema_filed, dialect):
    problem_statement = data["query"]
    issue_sql = data["issue_sql"]
    issue_sql_str = ""
    for sql in issue_sql:
        issue_sql_str += f"```sql\n{sql}\n```\n"
    if prompt_type == "baseline":
        if dialect.lower() == "oracle":
            return (
                baseline_v2.replace("[[SCHEMA]]", data[schema_filed])
                .replace("[[USER_ISSUE]]", problem_statement)
                .replace("[[ISSUE_SQL]]", issue_sql_str)
            )
        else:
            return (
                baseline_v1.replace("[[SCHEMA]]", data[schema_filed])
                .replace("[[USER_ISSUE]]", problem_statement)
                .replace("[[ISSUE_SQL]]", issue_sql_str)
            )
    else:
        raise ValueError(f"Invalid prompt type: {prompt_type}")


def generate_prompts(data_list, prompt_type, schema_field, dialect):
    prompt_list = []
    final_data_list = []

    # Use tqdm to show progress while generating prompts
    for data in tqdm(data_list, desc="Generating prompts"):
        prompt = generate_prompt(data, prompt_type, schema_field, dialect)
        prompt_list.append(prompt)
        final_data_list.append(data)

    return prompt_list, final_data_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate prompts for the SO-Evaluation task."
    )
    parser.add_argument("--data_path", type=str, help="Path to the data file.")
    parser.add_argument(
        "--prompt_path", type=str, help="Path to save the generated prompts."
    )
    parser.add_argument(
        "--prompt_type",
        type=str,
        help="Type of prompt to generate.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of instances to process.",
        default=None,
    )
    parser.add_argument(
        "--schema_field",
        type=str,
        required=True,
        help="Either original_schema or preprocess_schema",
    )
    parser.add_argument(
        "--dialect", type=str, required=True, help="dialect of the SQL engine"
    )
    args = parser.parse_args()

    data_list = load_jsonl(args.data_path)

    # final_data_list = filter_instances(data_list)
    final_data_list = data_list
    prompt_list, final_data_list = generate_prompts(
        final_data_list,
        args.prompt_type,
        args.schema_field,
        args.dialect,
    )

    if args.limit is not None:
        limit = args.limit
    else:
        limit = len(prompt_list)
    write_prompts(prompt_list, final_data_list, args.prompt_path, limit)
    print(f"Generated {len(prompt_list)} prompts.")
    print("Prompts generated successfully.")
