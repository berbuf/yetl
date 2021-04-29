import pandas as pd
import numpy as np

import yetl.etl.run_etl as run_etl

pd.set_option('mode.chained_assignment', None)


def select(flow, elem):
    if any([k in flow.columns for k in elem["select"]]):
        pass  # raise error
    flow = flow[elem["select"]]
    flow.columns = elem["as"]
    return flow, []


def where(flow, elem):
    cond = np.array([True] * len(flow))
    for src in elem["to"]:
        for f in elem["where"]:
            cond &= flow[src].apply(f).values
    return flow[cond], []


def check(flow, elem):
    track_out = []
    for src in elem["to"]:
        integrity = flow[src].apply(
            lambda x: run_etl.integrity_track(x, elem["check"]))
        flow, out = run_etl.two_tracks(flow, src, "check", integrity)
        track_out += out
    return flow, track_out


def apply(flow, elem):
    if "as" not in elem:
        elem["as"] = elem["to"]
    for src, dest in zip(elem["to"], elem["as"]):
        tmp = flow[src]
        for f in elem["apply"]:
            tmp = tmp.apply(f)
        flow[dest] = tmp
    return flow, []


def aggregation(flow, elem):
    index_dest = 0
    for src in elem["to"]:
        for f in elem["aggregation"]:
            flow[elem["as"][index_dest]] = flow[src].apply(f)
            index_dest += 1
    return flow, []


def groupby(flow, elem):
    rows = []
    cond = np.array([k != elem["groupby"] for k in flow.columns])
    for k, gr in flow.groupby(elem["groupby"]):
        e = gr.values.transpose()[cond]
        rows += [[k]+e.tolist()]
    flow = pd.DataFrame(rows, columns=flow.columns)
    return flow, []


fct_map = {
    "select": select,
    "where": where,
    "check": check,
    "apply": apply,
    "aggregation": aggregation,
    "groupby": groupby,
}


def run_steps(flow, process):
    track_out = []
    for elem in process:
        key = list(elem)[0]
        flow, out = fct_map[key](flow, elem)
        track_out += out
    return flow, track_out
