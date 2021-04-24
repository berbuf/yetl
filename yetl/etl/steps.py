import pandas as pd
import numpy as np

pd.set_option('mode.chained_assignment', None)


def select(flow, elem):
    if any([k in flow.columns for k in elem["select"]]):
        pass  # raise error
    flow = flow[elem["select"]]
    flow.columns = elem["as"]
    return flow, []


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
    print(flow)
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
    "apply": apply,
    "aggregation": aggregation,
    "groupby": groupby,
}


def run_steps(flow, process):
    track_out = []
    for elem in process:
        key = list(elem)[0]
        flow, out = fct_map[key](flow, elem)
    return flow, []
