from .file_in import run_file_in


def run_file_out(flow, process):
    return flow, []


def run_steps(flow, process):
    return flow, []


def run_etl_(etl):
    steps = [("file_in", run_file_in),
             ("steps", run_steps),
             ("file_out", run_file_out)]
    out_flow, flow = [], None
    for k, f in steps:
        flow, out = f(flow, etl[k])
        out_flow += out
    return flow, out_flow
