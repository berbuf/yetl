import pandas as pd
import psycopg2

import yetl.etl.run_etl as run_etl


def load_postgres(rows, columns, conflict_keys, table, connection):
    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            for row in rows:
                values = ", ".join(["'{}'".format(str(e)) for e in row])
                query = "INSERT INTO {} ({}) VALUES ( {} ) ON CONFLICT ({}) DO NOTHING;".format(
                    table, ", ".join(columns), values, ", ".join(conflict_keys))
                cur.execute(query)
        conn.commit()


def execute_psql(connection, cmd):
    with psycopg2.connect(connection) as conn:
        with conn.cursor() as curs:
            curs.execute(cmd)
        conn.commit()


postgres_type = {
    int: "INTEGER",
    float: "DOUBLE PRECISION",
    str: "VARCHAR NOT NULL",
}


def load_flow(flow, process) -> None:
    # break
    # if dump select
    elem = [e for e in process if "name" in e]
    columns = [e["name"][1] for e in elem]
    rows = flow[columns].values
    postgres = [e for e in process if "postgres" in e][0]
    conflict_keys, table, connection = postgres["conflict_keys"], postgres["table"], postgres["connection"]
    if postgres["create"]:
        type_ = [postgres_type[e["domain"][0]] for e in elem]
        schema = ",\n".join(["{} {}".format(c, t)
                             for c, t in zip(columns, type_)])
        cmd = "CREATE TABLE IF NOT EXISTS {}(\n{},\nprimary key({}))".format(
            table, schema, ", ".join(conflict_keys))
        execute_psql(connection, cmd)
    load_postgres(rows, columns, conflict_keys, table, connection)


def extract_flow(process):
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
        src, dest = elem["name"]
        if src not in flow.columns:
            pass  # raise error
        integrity = flow[src].apply(
            lambda x: run_etl.integrity_track(x, elem["domain"]))
        flow, out = run_etl.two_tracks(flow, src, "domain", integrity)
        flow.rename({src: dest}, axis=1, inplace=True)
        track_out += out
    return flow, track_out


def run_file_in(_, process):
    flow = extract_flow(process)
    flow, out = integrity_check(flow, process)
    return flow, out


def run_file_out(flow, process):
    flow, out = integrity_check(flow, process)
    load_flow(flow, process)
    return flow, out
