prompt_path="/prompts/0106_assistant_raw.jsonl"
# model="gpt-4o"
# model="claude"
model="gemini"
inter_output_path="assistant_output/inter_output/0106_${model}_assistant_raw_inter_output.jsonl"

python gpt_infer.py --prompt_path $prompt_path --output_path $inter_output_path --model_name $model


final_output_path="assistant_output/final_output/0106_${model}_assistant_raw_final_output.jsonl"
python assistant/post_process.py --input_path $inter_output_path --output_path $final_output_path 