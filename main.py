#from marquez_client import MarquezClient

#client = MarquezClient(url='http://localhost:5000')
#e = client.list_namespaces()

import pandas as pd

from yetl.yetl import run_etl


def get_df():
    # str, str_to_float, float, int
    col_1 = ["A", "B", 1, "D", "A", 1]
    col_2 = ["1.100,1", "10,2", "156", "42,42", "42,42", "42,42"]
    col_3 = [8.9, 10, 1, 14.5, 12., 12.]
    col_4 = [1, 2, 3, -4, 1, 1]
    e = list(zip(col_1, col_2, col_3, col_4))
    return pd.DataFrame(e, columns=["col_1", "col_2", "col_3", "col_4"])


df = get_df()
df.to_csv("./path.csv", sep=",")
# run_etl("etl_marquez.yaml")
run_etl("etl.yaml")

# TODO
"""
    X steps
    X add check, where
    X setup.py
    X file out
    move in out connectors to folder
    marquez
    add options
    type checking
    add errors
"""
