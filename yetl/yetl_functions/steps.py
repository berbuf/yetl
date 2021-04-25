

def str_to_float(x: str) -> float:
    return float(x)


def fr_format(x: str) -> str:
    return x.replace(".", "").replace(",", ".")


def mean_p90(x: [float]) -> float:
    return sum(x) / len(x)


def positive(x: float) -> bool:
    return x > 0


def rounded_even(x: float) -> bool:
    return not int(x) % 2
