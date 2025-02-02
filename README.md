# BIRD-CRITIC 1.0 (SQL)  <img src="materials/hku-logo.jpg" alt="HKU Logo" width="50" style="vertical-align:middle;margin-left:10px;"> <img src="materials/google-cloud-logo.png" alt="Google Cloud Logo" width="50" style="vertical-align:middle;margin-left:10px;">

<p align="center">
  <img src="materials/red_bird_single.webp" 
       style="width: 30%; min-width: 100px; display: block; margin: auto; border-radius: 50%; overflow: hidden;">
</p>



[![License](https://img.shields.io/badge/License-CC%20By%20SA%204.0-orange.svg)](https://creativecommons.org/licenses/by-sa/4.0/deed.en)
[![HuggingFace](https://img.shields.io/badge/Dataset-HuggingFace-FFD21E.svg)](https://huggingface.co/datasets/birdsql/bird-critic-1.0-flash-exp/)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-teal.svg)](https://www.python.org/downloads/release/python-310/)
[![OpenAI 1.40+](https://img.shields.io/badge/OpenAI-1.40+-beige.svg)](https://pypi.org/project/openai/)



## 🧸 Overview

BIRD-Critic 1.0 introduces a novel SQL benchmark designed to evaluate a key capability: **Can large language models (LLMs) diagnose and solve user issues within real-world database environments?**

The benchmark comprises 600 tasks for development and 200 held-out out-of-distribution (OOD) tests. BIRD-CRITIC 1.0 is built on realistic user issues across 4 prominent open-source SQL dialects: MySQL, PostgreSQL, SQL Server, and Oracle. It expands beyond simple SELECT queries to cover a wider range of SQL operations, reflecting actual application scenarios. Finally, an optimized execution-based evaluation environment is included for rigorous and efficient validation.

### ✅ Verification Process

Each task in BIRD-CRITIC has been verified by human experts on the following dimensions:

1) Reproduction of errors on the BIRD environment to prevent data leakage.
2) Carefully curated test case functions for each task specifically.
   - **Soft EX**: This metric can evaluate SELECT-ONLY tasks.
   - **Soft EX + Parsing**: This metric can evaluate tasks with user-specific requirements or refinements.
   - **Test Case**: For DBA tasks, such as CRUD (CREATE, READ, UPDATE, DELETE), test cases are designed to evaluate the correctness of the logic. This is also effective for user issues requiring multiple sequential SQL queries to resolve.
   - **Query Execution Plan**: For user tasks involving efficiency improvement or runtime errors, QEP (Query Execution Plan) can be used to evaluate solution SQL queries at the algorithm level.
3) Fast Eval Sandbox via PostgreSQL template & docker.
4) Created new RDBs in different scales and professional domains.

### 🐣 Lite Version

We are releasing a lite version of BIRD-Critic, `bird-critic-1.0-flash-exp`, which includes 200 high-quality user issues on PostgreSQL when developing real-world applications. We curate tasks by:
- Collecting and understanding realistic user issues.
- Distilling problem definitions and SQL knowledge.
- Reproducing bugs and solutions in the BIRD environment.
- Designing test cases for evaluation.

### Model Performance Results on BIRD-CRITIC 1.0 Flash

| Rank | Model Name | Score | Level |
|------|------------|-------|-----------|
| 1 | o1-preview-2024-09-12 | **38.5** | 🏆 Leading |
| 2 | deepseek-reasoner (r1) | 34.0 | 🌟 Elite |
| 3 | claude-3-5-sonnet | 24.0 | 🔸 Advanced |
| 4 | gemini-2.0-flash-exp | 24.0 | 🔸 Advanced |
| 5 | Qwen2.5-Coder-32B-Instruct | 23.5 | 🔸 Advanced |
| 6 | gemini-2.0-flash-thinking-exp | 19.5 | 🔸 Advanced |

> Full result can be found [here](https://huggingface.co/datasets/birdsql/bird-critic-1.0-flash-exp/)

## 🦅 Full Sets of BIRD-CRITIC 1.0

The BIRD-CRITIC 1.0 benchmark is available in the following configurations:

1.  `bird-critic-1.0-flash-exp`: A lite version consisting of 200 instances on PostgreSQL.
2.  `bird-critic-1.0-open`: The full version containing 600 instances across MySQL, PostgreSQL, SQL Server, and Oracle.
3.  `bird-critic-1.0-postgresql`: A 600-instance version specifically for PostgreSQL.
4.  `bird-critic-1.0-bigquery`: A lite version containing between 100 and 200 instances for BigQuery.

## 📦 Dataset Details

### Dataset Description

- **Database:** The database can be download from [the Google Drive](https://drive.google.com/drive/folders/1O4svFGkE8_Ps60EQeyrCTN6LVOWudjgm?usp=sharing). Check the [Quick Eval](#quick-eval) section for more details.
- **data:** Each data instance contain the following main parts:
   - `db_id`: The name of the database.  
   - `query`: The user query is rewritten in the BIRD environment.  
   - `error_sql`: The buggy SQL query written by the user.  
   - `sol_sql`: The ground truth SQL solution.  
   - `preprocess_sql`: SQL queries to run before executing the solution or prediction.  
   - `clean_up_sql`: SQL queries to run after the test cases to revert any changes made to the database.  
   - `test_cases`: A set of test cases to validate the predicted corrected SQL.
   - `efficiency`: True if this question needs optimization, measure the cost by Query Execution Plan (QEP)
   - `external_data`: For the external JSON data if present
- **baseline:** The baseline code is available in the [`./baseline`](./baseline) directory.
- **evaluation:** The evaluation code is available in the [`./evaluation`](./evaluation) directory.
- **Curated by:** BIRD Team & Google Cloud
- **License:** [cc-by-sa-4.0](https://creativecommons.org/licenses/by-sa/4.0/)
- **HuggingFace Dataset Card:** [bird-critic-1.0-flash-exp](https://huggingface.co/datasets/birdsql/bird-critic-1.0-flash-exp)

### Dataset Uses

To avoid data leakage by auto-crawling, we do not include GT solution sqls and test cases along with data.
please email [bird.bench23@gmail.com](mailto:bird.bench23@gmail.com) or [bird.bench25@gmail.com](mailto:bird.bench25@gmail.com) for full set, which will be sent automatically.


### Use the Dataset from HuggingFace
```python
from datasets import load_dataset

dataset = load_dataset("birdsql/bird-critic-1.0-flash-exp")

print(dataset["flash"][0])
```

## 💨 Quick Eval
### Generation
To run the baseline code you need to install the following dependencies:
```bash
cd baseline
conda create -n bird_critic python=3.10 -y
conda activate bird_critic
pip install -r requirements.txt
```

You also need to setup the model name (eg., **gpt-4o-2024-08-06**) with the API key in the `config.py` file. Then you can run the following command to generate the output:
```bash
# Generate the prompt
cd run
bash generate_prompt.sh

# LLM Inference, need to set the API key in config.py
bash run_baseline.sh
```
The output will be save in the [`./baseline/outputs/final_output/`](./baseline/outputs/final_output/)


### Evaluation
We use **docker** to provide a consistent environment for running the benchmark. To set up the environment, follow these steps:

1. First download the PostgreSQL database from [the Google Drive](https://drive.google.com/drive/folders/1O4svFGkE8_Ps60EQeyrCTN6LVOWudjgm?usp=sharing).
2. Unzip the folder and save it in the [`./evaluation`](./evaluation) named with postgre_table_dumps
3. Build the docker compose
```bash
cd evaluation
docker compose up --build
```
4. Interact with the PostgreSQL database
Use the `perform_query_on_postgresql_databases()` function in the `evaluation/src/db_utils.py` file to interact with the PostgreSQL database. `query` is the SQL query you want to run, and `db_name` is the name of the database you want to run the query on. The function will return the result of the query.
5. Run the evaluation script inside the so_eval_env container
```bash
docker compose exec so_eval_env bash
cd run
bash run_eval.sh 
```
The output will be save in the [`./evaluation/outputs/`](./evaluation/outputs/)
If you want the log file for each instance, you can set the `--logging` to `true` in the `run_eval.sh` script.

## 📋 Todo Lists

- [x] Release lite version, bird-critic-1.0-flash (200).
- [x] Open source code, leaderboard page.
- [ ] Release Full bird-critic-1.0-open (600 w/ 5 dialects).
- [ ] Release Full bird-critic-1.0-postgresql (600 pg tasks).
- [ ] Update agent baselines.
- [ ] BIRD-Pro v0.5
- [ ] BIRD-CRITIC 1.5 / 2.0 on track!

## Created By:
BIRD Team & Google Cloud
