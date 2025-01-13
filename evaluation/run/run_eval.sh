# jsonl_file="JSONL_FILE_FROM_INFERENCE"
jsonl_file="/app/temp/0110_gpt-4o_final_output.jsonl"
python3 /app/src/evaluation.py --jsonl_file "$jsonl_file" 