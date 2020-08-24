import ast
import re


def clean_str(var):
    return re.sub('[^0-9a-zA-Z]+', ' ', str(var).lower().strip()).strip() if var else None


def lower_keys(obj: dict):
    return {k.lower(): v for k, v in obj.items()}


def clean_keys(obj: dict):
    return {clean_str(k): v for k, v in (obj or {}).items()}


def lower_strings(obj: dict):
    if isinstance(obj, dict):
        return {lower_strings(k): lower_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [lower_strings(v) for v in obj]
    if isinstance(obj, str):
        return obj.lower()
    else:
        return obj


def eval_str(s):
    try:
        return ast.literal_eval(s) if s and isinstance(s, str) else s
    except:
        return s
