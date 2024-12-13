processed_data="/app/agentless/output/validation_output/output_filtered.jsonl"
output_folder='/app/agentless/output/final_output'
num_threads=8
dialect='mysql'
python /app/agentless/validation/run_test.py --processed_data $processed_data --output_folder $output_folder --num_threads $num_threads --dialect $dialect