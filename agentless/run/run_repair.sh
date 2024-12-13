max_samples=5
loc_file="/app/agentless/data/all_data_clean.jsonl"
model="deepseek-chat"
backend="deepseek"
output_folder='/app/agentless/output/repair_output'
num_threads=8

python /app/agentless/repair/repair.py --loc_file $loc_file --model $model --backend $backend --output_folder $output_folder --num_threads $num_threads --max_samples $max_samples