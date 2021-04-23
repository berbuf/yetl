
from .file_in import resolve_in


def resolve_out(block):
    pass


def resolve_steps(block):
    pass


def resolve_etl(etl):
    steps = [(resolve_in, "file_in"),
             (resolve_out, "file_out"),
             (resolve_steps, "steps")]
    for f, k in steps:
        etl[k] = f(etl[k])
    return etl
