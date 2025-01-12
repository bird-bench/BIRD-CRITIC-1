prompt_path="./prompts/0110_assistant_raw.jsonl"
# model="gpt-4o"
model="claude"
# model="gemini"
# model="deepseek"
inter_output_path="./outputs/inter_output/0110_${model}_assistant_raw_inter_output.jsonl"

python ./call_api.py --prompt_path $prompt_path --output_path $inter_output_path --model_name $model


final_output_path="./outputs/final_output/0110_${model}_assistant_raw_final_output.jsonl"
python ./post_process.py --input_path $inter_output_path --output_path $final_output_path 