data_path="../data/0110_data.jsonl"
assistant_prompt_path="../prompts/0110_assistant.jsonl"
python ../src/prompt_generator.py --data_path $data_path --prompt_path $assistant_prompt_path --prompt_type "assistant"