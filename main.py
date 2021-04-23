import pandas as pd

from yetl.yetl import run_etl


def get_df():
    # str, str_to_float, float, int
    col_1 = ["A", "Z", 1, "D"]
    col_2 = ["1.100,1", "10,2", "156", "42.42"]
    col_3 = [8.9, 10, 1, 14.5]
    col_4 = [1, 2, 3, -4]
    e = list(zip(col_1, col_2, col_3, col_4))
    return pd.DataFrame(e, columns=["col_1", "col_2", "col_3", "col_4"])


df = get_df()
df.to_csv("./path.csv", sep=",")
run_etl()
