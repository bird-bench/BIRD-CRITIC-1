data_path="../data/xx.jsonl"
assistant_prompt_path="../prompts/xx.jsonl"
dialect="postgresql"
schema_filed="preprocess_schema"
python ../src/prompt_generator.py --data_path $data_path --prompt_path $assistant_prompt_path --prompt_type "baseline" \
