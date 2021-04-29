import numpy as np

from .in_out import run_file_in, run_file_out
from .steps import run_steps


def integrity_track(x, fcts: [callable]) -> (bool, str):
    ret, track = True, ""
    for f in fcts:
        if ret:
            track = "{}{}/".format(track, f.__name__)
        ret &= type(x) == f if isinstance(f, type) else f(x)
    return ret, track


def two_tracks(flow, col, step, integrity):
    cond, log = list(zip(*integrity))
    cond = np.array(cond)
    out_col = [col] * sum(~cond)
    out_step = [step] * sum(~cond)
    out_val = flow[col][~cond].values
    out_log = np.array(log)[~cond]
    return flow[cond], list(zip(out_col, out_val, out_step, out_log))


def run_etl_(etl):
    out_flow, flow = [], None
    for k, f in [("file_in", run_file_in),
                 ("steps", run_steps),
                 ("file_out", run_file_out)]:
        flow, out = f(flow, etl[k])
        out_flow += out
    return flow, out_flow
