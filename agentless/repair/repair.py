import sys

parent_dir = "/app/agentless"
sys.path.append(parent_dir)
import json
import os
import re
import argparse
import concurrent.futures
from prompt import repair_prompt_combine_topn
from util.llm import make_model
from util.utils import load_jsonl, setup_logger, cleanup_logger
from tqdm import tqdm


def process_loc(loc, args):
    instance_id = loc["instance_id"]
    log_file = os.path.join(args.output_folder, "repair_logs", f"{instance_id}.log")
    logger = setup_logger(log_file)
    logger.info(f"================ repairing {instance_id} ================")
    prompt_template = repair_prompt_combine_topn
    message = prompt_template.format(
        problem_statement=loc["query"],
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

    # sample_responses = list({v["response"]: v for v in sample_responses}.values())

    with open(args.output_file, "a") as f:
        loc["pred_sols"] = sample_responses
        f.write(json.dumps(loc) + "\n")
    cleanup_logger(logger)


def repair(args):
    with open(f"{args.output_folder}/args.json", "w") as f:
        json.dump(vars(args), f, indent=4)

    locs = load_jsonl(args.loc_file)

    if args.num_threads == 1:
        for loc in tqdm(locs, total=len(locs), colour="MAGENTA"):
            process_loc(loc, args)
    else:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.num_threads
        ) as executor:
            futures = {executor.submit(process_loc, loc, args): loc for loc in locs}
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(locs),
                colour="MAGENTA",
            ):
                future.result()


# def post_process(args):
#     locs = load_jsonl(args.output_file)
#     for loc in locs:
#         if "pred_sols" in loc:
#             pred_sqls = []
#             for pred_sol in loc["pred_sols"]:
#                 if "response" in pred_sol:
#                     pred_sql = extract_sql_queries(pred_sol["response"])
#                     if pred_sql:
#                         pred_sqls.append(pred_sql)
#             loc["pred_sqls"] = pred_sqls
#     args.post_process_file = args.output_file.replace(".jsonl", "_post_processed.jsonl")
#     with open(args.post_process_file, "w") as f:
#         for loc in locs:
#             f.write(json.dumps(loc) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_samples", type=int, default=10, help="Sampling budget.")
    parser.add_argument("--loc_file", type=str, required=True)
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
    if not os.path.exists(os.path.join(args.output_folder, "repair_logs")):
        os.makedirs(os.path.join(args.output_folder, "repair_logs"))

    args.output_file = os.path.join(args.output_folder, "output.jsonl")

    repair(args)
    # post_process(args)


if __name__ == "__main__":
    main()
