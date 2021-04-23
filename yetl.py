import yaml

from resolve.resolve_etl import resolve_etl
from etl.run_etl import run_etl_


from yetl_functions import domains
from yetl_functions import steps
import_map = {
    "domains": domains,
    "steps": steps,
}


def get_fct(module, name):
    if name in __builtins__:
        return __builtins__[name]
    try:
        return getattr(import_map[module], name)
    except:
        raise Exception("function {} is unknown".format(name))


def get_json(yaml_path):
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


def run_etl(yaml_path="etl.yaml"):
    try:
        etl = get_json(yaml_path)
        etl = resolve_etl(etl)  # compile
        etl = run_etl_(etl)  # run
    except Exception as e:
        print(e)
