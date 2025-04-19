#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
from datasets import load_dataset


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate JSONL files sliced by SQL dialect based on schema and input data"
    )
    parser.add_argument(
        "--schema_path", required=True, help="Path to the schema JSONL file"
    )
    parser.add_argument(
        "--input_path", default="", help="Path to the input JSONL file (may be empty)"
    )
    parser.add_argument(
        "--output_folder",
        required=True,
        help="Directory where output files will be written",
    )
    return parser.parse_args()


def load_jsonl(path):
    """Read a JSONL file and return a list of dicts."""
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data.append(json.loads(line))
    return data


def dump_jsonl(data_list, out_path):
    """Write a list of dicts to a JSONL file."""
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in data_list:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main():
    args = parse_args()

    # 1. Prepare the input data list
    if (
        not args.input_path
        or not os.path.isfile(args.input_path)
        or os.path.getsize(args.input_path) == 0
    ):
        # If input_path is empty, missing, or zero-size, load default dataset
        dataset = load_dataset("birdsql/bird-critic-1.0-open")
        data_list = dataset["open"]
    else:
        data_list = load_jsonl(args.input_path)

    # 2. Read the schema and build a mapping by instance_id
    schema_list = load_jsonl(args.schema_path)
    schema_map = {item["instance_id"]: item for item in schema_list}

    # 3. Merge schema fields and input fields per record
    combined = []
    for record in data_list:
        iid = record.get("instance_id")
        schema_rec = schema_map.get(iid, {})
        # Merge: record fields take precedence over schema fields
        merged = {**schema_rec, **record}
        combined.append(merged)

    # 4. Group by dialect and write out files
    os.makedirs(args.output_folder, exist_ok=True)
    # Output config: dialect -> (filename prefix, number of records)
    config = {
        "MySQL": ("mysql_100", 100),
        "SQLServer": ("mssql_100", 100),
        "Oracle": ("oracle_100", 100),
        "PostgreSQL": ("postgresql_300", 300),
    }

    for dialect, (fname, limit) in config.items():
        subset = [rec for rec in combined if rec.get("dialect") == dialect]
        if len(subset) < limit:
            print(
                f"Warning: only {len(subset)} records available for dialect '{dialect}', expected {limit}. Writing all available records."
            )
            out_records = subset
        else:
            out_records = subset[:limit]

        out_path = os.path.join(args.output_folder, f"{fname}.jsonl")
        dump_jsonl(out_records, out_path)
        print(f"Wrote {len(out_records)} records to {out_path}")


if __name__ == "__main__":
    main()
