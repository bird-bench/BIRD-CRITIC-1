import json
import os
import argparse
from tqdm import tqdm
from prompt import assistant_prompt


# Utility functions
def load_jsonl(file_path):
    with open(file_path, "r") as file:
        return [json.loads(line) for line in file]


def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def write_prompts(prompts, data_list, prompt_path):
    create_directory(os.path.dirname(prompt_path))
    with open(prompt_path, "w") as f:
        for i, instance in enumerate(data_list):
            instance["prompt"] = prompts[i]
            f.write(json.dumps(instance, ensure_ascii=False) + "\n")


def generate_prompts(data_list, prompt_type):
    prompt_list = []
    final_data_list = []

    # Use tqdm to show progress while generating prompts
    for data in tqdm(data_list, desc="Generating prompts"):
        if prompt_type == "assistant":
            prompt_list.append(assistant_prompt(data))
            final_data_list.append(data)
        else:
            raise ValueError(f"Invalid prompt type: {prompt_type}")
    return prompt_list, final_data_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate prompts for the SO-Evaluation task."
    )
    parser.add_argument("--data_path", type=str, help="Path to the data file.")
    parser.add_argument(
        "--prompt_path", type=str, help="Path to save the generated prompts."
    )
    parser.add_argument(
        "--prompt_type",
        type=str,
        help="Type of prompt to generate.",
    )
    args = parser.parse_args()

    data_list = load_jsonl(args.data_path)

    # final_data_list = filter_instances(data_list)
    final_data_list = data_list
    prompt_list, final_data_list = generate_prompts(final_data_list, args.prompt_type)

    # prompt_list = prompt_list[:3]
    # final_data_list = final_data_list[:3]
    write_prompts(prompt_list, final_data_list, args.prompt_path)
    print(f"Generated {len(prompt_list)} prompts.")
    print("Prompts generated successfully.")
