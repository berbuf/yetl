
from .domains import resolve_domains
from .steps import resolve_steps


def resolve_etl(etl):
    steps = [(lambda x: resolve_domains(x, "in"), "file_in"),
             (lambda x: resolve_domains(x, "out"), "file_out"),
             (resolve_steps, "steps")]
    for f, k in steps:
        etl[k] = f(etl[k])
    return etl
