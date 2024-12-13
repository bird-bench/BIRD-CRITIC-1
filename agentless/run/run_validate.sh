max_samples=5
repair_file="/app/agentless/output/repair_output/output.jsonl"
model="deepseek-chat"
backend="deepseek"
output_folder='/app/agentless/output/validation_output'
num_threads=8

python /app/agentless/validation/validation.py --repair_file $repair_file --model $model --backend $backend --output_folder $output_folder --num_threads $num_threads --max_samples $max_samples