import pandas as pd

import yetl.etl.run_etl as run_etl


def init_flow(process):
    path = [e for e in process if "path" in e][0]
    if path["format"] == "csv":
        flow = pd.read_csv(path["path"], sep=path["sep"])
    else:
        raise Exception("only csv")
    return flow


def integrity_check(flow, process):
    track_out = []
    for elem in process:
        if "name" not in elem:
            continue
        if elem["name"] not in flow.columns:
            pass  # raise error
        integrity = flow[elem["name"]].apply(
            lambda x: run_etl.integrity_track(x, elem["domain"]))
        flow, out = run_etl.two_tracks(flow, elem["name"], "domain", integrity)
        track_out += out
    return flow, track_out


def run_file_in(_, process):
    flow = init_flow(process)
    flow, out = integrity_check(flow, process)
    return flow, out
