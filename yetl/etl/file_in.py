import pandas as pd
import numpy as np


def init_flow(process):
    path = [e for e in process if "path" in e][0]
    if path["format"] == "csv":
        flow = pd.read_csv(path["path"], sep=path["sep"])
    else:
        raise Exception("only csv")
    return flow


def domain_check(x, process) -> (bool, [str]):
    ret, track = True, ""
    for f in process["domain"] + ([process["in"]] if "in" in process else []):
        if ret:
            track = "{}{}/".format(track, f.__name__)
        ret &= type(x) == f if isinstance(f, type) else f(x)
    return ret, track


def two_tracks(flow, col, integrity):
    cond, log = list(zip(*integrity))
    cond = np.array(cond)
    out_col = [col] * sum(~cond)
    out_val = flow[col][~cond].values
    out_log = np.array(log)[~cond]
    return flow[cond], list(zip(out_col, out_val, out_log))


def integrity_check(flow, process):
    track_out = []
    for col in [e for e in process if "name" in e]:
        if col["name"] not in flow.columns:
            pass  # raise error
        integrity = flow[col["name"]].apply(lambda x: domain_check(x, col))
        flow, out = two_tracks(flow, col["name"], integrity)
        track_out += out
    return flow, track_out


def run_file_in(_, process):
    flow = init_flow(process)
    flow, out = integrity_check(flow, process)
    return flow, out
