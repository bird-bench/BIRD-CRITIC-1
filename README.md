# BIRD-CRITIC-1.0-Flash

<p align="center">
  <img src="materials/red_bird_single.webp" 
       style="width: 30%; min-width: 100px; display: block; margin: auto; border-radius: 50%; overflow: hidden;">
</p>



[![License](https://img.shields.io/badge/License-CC%20By%20NC%204.0-orange.svg)](https://creativecommons.org/licenses/by-nc/4.0/)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-teal.svg)](https://www.python.org/downloads/release/python-310/)
[![OpenAI 1.40+](https://img.shields.io/badge/OpenAI-1.40+-beige.svg)](https://pypi.org/project/openai/)



BIRD-Critic is the first SQL debugging benchmark designed to answer a critical question:
**Can large language models (LLMs) fix user issues in real-world database applications?** \
Each task in BIRD-CRITIC has been verified by human experts on the following dimensions:
1) Reproduction of errors on BIRD env to prevent data leakage.
2) Carefully curate test case functions for each task specifically. 
   - **Soft EX**: This metric can evaluate SELECT-ONLY tasks.
   - **Soft EX + Parsing**: This metric can evaluate tasks with user specific requirements or refinements.
   - **Test Case**: For DBA tasks, such as CRUD (CREAT, READ, UPDATE, DELET), test cases should be promised to evaluate the correct logic. This is also effective for user issues requiring multiple sequential SQLs to resolve. 
   - **Query Execution Plan**: For user tasks involving efficiency improvement or runtime errors, QEP can be introduced to evaluate solution SQLs on algorithm level.
4) Fast Eval Sandbox via PostgreSQL template & docker.
5) Created new RDBs in different scale and professional domains.

We are releasing a lite version of BIRD-Critic, `bird-critic-1.0-flash-exp`, which includes 200 high-quality user issues on PostgreSQL when developing real-world applications. We curate tasks by:
- Collecting and understanding realistic user issues.
- Distilling problem definitions and SQL knowledge.
- Reproducing bugs and solutions in the BIRD environment.
- Designing test cases for evaluation.


## Dataset Details

### Dataset Description

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

print(dataset["train"][0])
```

## Code Usage
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

1. First download the BIRD Dev PostgreSQL database from [the Google Drive](https://drive.google.com/drive/folders/1O4svFGkE8_Ps60EQeyrCTN6LVOWudjgm?usp=sharing).
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
