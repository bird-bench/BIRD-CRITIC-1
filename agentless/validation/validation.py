import sys

parent_dir = "/app/agentless"
sys.path.append(parent_dir)
import json
import os
import re
import io
import ast
import argparse
import concurrent.futures
from prompt import generate_tests_prompt_template
from contextlib import redirect_stdout
from util.llm import make_model
from util.utils import (
    load_jsonl,
    setup_logger,
    cleanup_logger,
    remove_comments_and_docstrings,
    rename_function_to_test_func,
)
from tqdm import tqdm
from util.mysql_setup import perform_query_on_mysql_databases


def extract_first_code_block(text):
    pattern = re.compile(r"```python(.*?)```", re.DOTALL)

    match = pattern.search(text)

    if match:
        return match.group(1).strip()

    return None


def gen_test(repair_data, args):
    instance_id = repair_data["instance_id"]
    log_file = os.path.join(
        args.output_folder, "generating_test_logs", f"{instance_id}.log"
    )
    logger = setup_logger(log_file)

    logger.info(f"================ generating test for {instance_id} ================")

    problem_statement = repair_data["query"]

    prompt_template = generate_tests_prompt_template
    message = prompt_template.format(
        problem_statement=problem_statement,
    ).strip()

    logger.info(f"prompting with message:\n{message}")
    sample_responses = []

    # get greedy sample
    model = make_model(
        model=args.model,
        logger=logger,
        backend=args.backend,
        max_tokens=1024,
        temperature=0,
        batch_size=1,
    )
    greedy_traj = model.codegen(
        message, num_samples=1, prompt_cache=args.max_samples > 1
    )[0]
    sample_responses.append(greedy_traj)
    model = make_model(
        model=args.model,
        logger=logger,
        backend=args.backend,
        max_tokens=1024,
        temperature=0.8,
        batch_size=args.max_samples - 1,  # minus the 1 greedy sample
    )
    if args.max_samples - 1:
        # always use cached prompt if possible for later samples
        sample_trajs = model.codegen(
            message, num_samples=args.max_samples - 1, prompt_cache=True
        )
    sample_responses.extend(sample_trajs)
    # filter out the duplicate responses
    sample_responses = list({v["response"]: v for v in sample_responses}.values())

    with open(args.output_file, "a") as f:
        repair_data["gen_test_cases"] = sample_responses
        f.write(json.dumps(repair_data) + "\n")
    cleanup_logger(logger)


def generate_tests(args):
    with open(f"{args.output_folder}/args.json", "w") as f:
        json.dump(vars(args), f, indent=4)

    repairs = load_jsonl(args.repair_file)

    if args.num_threads == 1:
        for repair_data in tqdm(repairs, total=len(repairs), colour="MAGENTA"):
            gen_test(repair_data, args)
    else:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.num_threads
        ) as executor:
            futures = {
                executor.submit(gen_test, repair_data, args): repair_data
                for repair_data in repairs
            }
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(repairs),
                colour="MAGENTA",
            ):
                future.result()


def normalize_test(test: str):
    def normalize_code(code):
        try:
            node = ast.parse(code)
            return ast.unparse(node)
        except:
            return code

    test = extract_first_code_block(test)
    test = normalize_code(test)

    try:
        remove_docstring_test = remove_comments_and_docstrings(test)
        ast.parse(remove_docstring_test)  # check
    except:
        remove_docstring_test = test

    try:
        remove_docstring_test = rename_function_to_test_func(remove_docstring_test)
    except:
        pass

    return remove_docstring_test


def normalize_tests(args):
    tests = load_jsonl(f"{args.output_folder}/output.jsonl")
    for d in tests:
        instance_id = d["instance_id"]
        log_file = os.path.join(
            args.output_folder, "generating_test_logs", f"{instance_id}.log"
        )
        logger = setup_logger(log_file)
        logger.info(
            f"================ normalizing tests for {instance_id} ================"
        )

        d_test_list = d["gen_test_cases"]
        d_test_norm_list = []
        for i, test in enumerate(d_test_list):
            normalized = normalize_test(test["response"])
            d_test_list[i] = normalized
            d_test_norm_list.append(normalized)
        d["normalized_tests"] = d_test_norm_list
        logger.info(f"normalized tests: {d_test_norm_list}")
        cleanup_logger(logger)

    with open(f"{args.output_folder}/output_normalized.jsonl", "w") as f:
        for d in tests:
            f.write(json.dumps(d) + "\n")


def filter_tests(args):
    tests = load_jsonl(f"{args.output_folder}/output_normalized.jsonl")
    for data in tests:
        instance_id = data["instance_id"]
        log_file = os.path.join(
            args.output_folder, "generating_test_logs", f"{instance_id}.log"
        )
        logger = setup_logger(log_file)
        logger.info(
            f"================ filtering tests for {instance_id} ================"
        )

        error_sql = data["error_sql"][0]
        db = data["db_id"]
        test_command = f"test_func('{error_sql}', '{db}')"

        kept_tests = []
        for test in data["normalized_tests"]:
            test_code = test
            logger.info(
                f"Executing test code:\n{test_code}\nwith command: {test_command}"
            )

            buf = io.StringIO()
            with redirect_stdout(buf):
                try:
                    exec(test_code + "\n" + test_command, globals(), locals())
                except Exception as e:
                    # redirect to buffer
                    print(e)

            output = buf.getvalue()
            # print(output)
            logger.info(f"Test output:\n{output}")
            if "Issue reproduced" in output:
                kept_tests.append(test_code)

        data.pop("normalized_tests", None)
        data.pop("gen_test_cases", None)
        data["filtered_test_cases"] = kept_tests
        logger.info(f"filtered tests: {kept_tests}")
        cleanup_logger(logger)

    with open(f"{args.output_folder}/output_filtered.jsonl", "w") as f:
        for d in tests:
            f.write(json.dumps(d) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_samples", type=int, default=10, help="Sampling budget.")
    parser.add_argument("--repair_file", type=str, required=True)
    parser.add_argument(
        "--model",
        type=str,
        default="deepseek-chat",
        choices=[
            "gpt-4o-2024-05-13",
            "deepseek-chat",
            "gpt-4o-mini-2024-07-18",
            "claude-3-5-sonnet-20241022",
        ],
    )
    parser.add_argument(
        "--backend",
        type=str,
        default="openai",
        choices=["openai", "deepseek", "anthropic"],
    )
    parser.add_argument("--output_folder", type=str, required=True)
    parser.add_argument(
        "--num_threads",
        type=int,
        default=1,
        help="Number of threads to use for creating API requests",
    )

    args = parser.parse_args()

    assert (not "deepseek" in args.model) or (
        args.backend == "deepseek"
    ), "Must specify `--backend deepseek` if using a DeepSeek model"

    if not os.path.exists(args.output_folder):
        os.makedirs(args.output_folder)
    if not os.path.exists(os.path.join(args.output_folder, "generating_test_logs")):
        os.makedirs(os.path.join(args.output_folder, "generating_test_logs"))

    args.output_file = os.path.join(args.output_folder, "output.jsonl")
    # generate_tests(args)
    # normalize_tests(args)
    filter_tests(args)


if __name__ == "__main__":
    main()
