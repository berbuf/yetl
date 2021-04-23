import numpy as np


def upper(x: str) -> bool:
    return x.isupper()


def float_fr(x: str) -> bool:
    try:
        float(x.replace(".", "").replace(",", ""))
    except:
        return False
    return True


def positive(x: float) -> bool:
    return x > 0
