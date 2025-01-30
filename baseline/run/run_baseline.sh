prompt_path="../prompts/assistant.jsonl"
model="gpt-4o-2024-0806"
inter_output_path="../outputs/inter_output/${model}_assistant_inter_output.jsonl"
mkdir -p ../outputs/inter_output
python ../src/call_api.py --prompt_path $prompt_path --output_path $inter_output_path --model_name $model


final_output_path="../outputs/final_output/${model}_assistant_final_output.jsonl"
mkdir -p ../outputs/final_output
python ../src/post_process.py --input_path $inter_output_path --output_path $final_output_path 