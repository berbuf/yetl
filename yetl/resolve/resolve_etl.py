
from .file_in import resolve_in
from .steps import resolve_steps


def resolve_out(block):
    pass


def resolve_etl(etl):
    steps = [(resolve_in, "file_in"),
             (resolve_out, "file_out"),
             (resolve_steps, "steps")]
    for f, k in steps:
        etl[k] = f(etl[k])
    return etl
