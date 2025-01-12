# Bird Critic

<p align="center">
  <img src="materials/red_bird_single.webp" 
       style="width: 30%; min-width: 100px; display: block; margin: auto; border-radius: 50%; overflow: hidden;">
</p>



[![License](https://img.shields.io/badge/License-CC%20By%20NC%204.0-orange.svg)](https://creativecommons.org/licenses/by-nc/4.0/)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-teal.svg)](https://www.python.org/downloads/release/python-310/)
[![OpenAI 1.40+](https://img.shields.io/badge/OpenAI-1.40+-beige.svg)](https://pypi.org/project/openai/)

## Overview
**BIRD-Critic** is the first SQL debugging benchmark designed to answer a critical question: *Can large language models (LLMs) resolve real-world user bugs in database applications?* To create this benchmark, we curated realistic bug cases from StackOverflow.

We are releasing a preview version of BIRD-Critic, which includes **141 data instances**. These data instances focuses on Stack Overflow issues, where we explore and collect SQL debugging scenarios, distill problem definitions, reproduce bugs and solutions in the BIRD environment, and design test cases for evaluation. 

## Dataset Introduction

For the preview version, we use the **BIRD-SQL dev database**, see more information on [the official BIRD website](https://bird-bench.github.io/).

Each data instance includes the following key components:
  - `db_id`: The name of the database.
  - `query`: The user query rewritten in the BIRD environment.
  - `error_sql`: The buggy SQL query written by the user.
  - `sol_sql`: The ground truth SQL solution.
  - `preprocess_sql`: List of SQL queries to execute before solution_sql/prediction.
  - `clean_up_sql`: SQL queries to execute after the test cases, to revise any effect made on the database .
  - `test_cases`: A set of test cases to validate the predicted corrected SQL.

## Environment Setup
### Generation
To run the baseline code you need to install the following dependencies:
```bash
cd baseline
conda create -n bird_critic python=3.10 -y
conda activate bird_critic
pip install -r requirements.txt
# Generate the prompt
bash generate_prompt.sh

# Inference, need to set the API key in config.py
bash run_baseline.sh
```
The output will be save in the [`./baseline/outputs/final_output/`](./baseline/outputs/final_output/)

### Evaluation
We use **docker** to provide a consistent environment for running the benchmark. To set up the environment, follow these steps:

1. First download the BIRD Dev PostgreSQL database from [the Google Drive](https://drive.google.com/drive/folders/1O4svFGkE8_Ps60EQeyrCTN6LVOWudjgm?usp=sharing).
2. Unzip the folder and save it in the [`./evaluation`](./evaluation) named with postgre_table_dumps
3. Build the docker compose
```bash
cd evaluation
docker compose up --build
```
4. Run the evaluation script inside the so_eval_env container
```bash
docker compose exec so_eval_env bash
# You need to modify the JSONL location in the run_eval.sh
bash run_eval.sh 
```
The output will be save in the [`./evaluation/outputs/`](./evaluation/outputs/)
If you want the log file for each instance, you can set the `--logging` to `true` in the `run_eval.sh` script.
