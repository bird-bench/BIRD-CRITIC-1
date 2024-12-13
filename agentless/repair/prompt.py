repair_prompt_combine_topn = """We are currently solving the following issue within the user query. Here is the user query:
--- BEGIN ISSUE ---
{problem_statement}
--- END ISSUE ---


Please generate the solution SQL query to fix the issue.
If there are multiple steps to solve the issue, please provide all the steps and separate them with a semicolon (;).

Wrap the solution SQL query with ```sql...```"""
