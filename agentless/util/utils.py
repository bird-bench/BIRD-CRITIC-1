import json
import logging
import os
import tokenize
import io
import re
import ast


def load_jsonl(filepath):
    """
    Load a JSONL file from the given filepath.

    Arguments:
    filepath -- the path to the JSONL file to load

    Returns:
    A list of dictionaries representing the data in each line of the JSONL file.
    """
    with open(filepath, "r") as file:
        return [json.loads(line) for line in file]


def setup_logger(log_file):
    logger = logging.getLogger(log_file)
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    return logger


def cleanup_logger(logger):
    handlers = logger.handlers[:]
    for handler in handlers:
        logger.removeHandler(handler)
        handler.close()


def remove_comments_and_docstrings(source_code: str) -> str:
    """
    Remove all comments and docstrings from the given Python source code string.

    Args:
        source_code (str): The Python source code as a string.

    Returns:
        str: The source code with all comments and docstrings removed.
    """
    # Use the tokenize module to parse the source code
    tokens = tokenize.generate_tokens(io.StringIO(source_code).readline)
    result_tokens = []
    prev_tok_type = tokenize.INDENT
    last_line = None
    last_col = 0
    # A state to track if we are inside a string that might be a docstring
    # We'll remove all triple-quoted strings that appear at the module, class, or function level
    # A docstring is considered if it's a STRING token that starts at the beginning of a logical line
    # or immediately after a colon that introduces a class/function definition.

    for tok_type, tok_str, start, end, line in tokens:
        # Check if token is a comment
        if tok_type == tokenize.COMMENT:
            # Skip comments
            continue

        # Check if token is a string (possible docstring)
        if tok_type == tokenize.STRING:
            # A docstring is typically a STRING token that appears:
            # - As the first token in a module
            # - As the first token in a class or function definition
            # For a simplistic approach, we can remove any triple-quoted strings
            # that appear where docstrings are expected:

            # Heuristic: if previous token type is INDENT, or the token starts at col 0,
            # and the string spans multiple lines (triple quotes), consider it a docstring.
            if (
                prev_tok_type == tokenize.INDENT
                or prev_tok_type == tokenize.NEWLINE
                or prev_tok_type == tokenize.DEDENT
            ) and (tok_str.startswith(('"""', "'''")) and "\n" in tok_str):
                # This is likely a docstring, skip it
                continue

            # Otherwise, it's a normal string (not a docstring), keep it
            result_tokens.append((tok_type, tok_str))
        else:
            # Not a comment or docstring, keep the token
            result_tokens.append((tok_type, tok_str))

        prev_tok_type = tok_type
        last_line, last_col = end

    # Convert token list back to source code
    # We only have token type and string, ignoring start/end positions is fine for this purpose
    new_code = tokenize.untokenize(result_tokens)
    return new_code


def rename_function_to_test_func(source_code: str) -> str:
    """
    Rename the first top-level function defined in the given source code to 'test_func'.

    Args:
        source_code (str): The Python source code as a string.

    Returns:
        str: The source code with the function name renamed to 'test_func'.
    """
    # Parse the source code into an AST
    tree = ast.parse(source_code)

    # Iterate over top-level nodes in the AST
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            # Rename the first function we encounter
            node.name = "test_func"
            break

    # Unparse the modified AST back to source code
    # ast.unparse is available in Python 3.9+
    # If you're using Python <3.9, you can use 'astor' library for unparse.
    modified_code = ast.unparse(tree)
    return modified_code
