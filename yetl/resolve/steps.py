import numpy as np
from yetl import yetl


# select
###
def parse_select(elem):
    src = elem["select"].split(" ")
    if len(np.unique(src)) != len(src):
        pass  # raise error
    return src


# select, apply
###
def parse_as(elem, key_word):
    src = elem[key_word].split(" ")
    dest = elem["as"].split(" ")
    if len(src) != len(dest):
        pass  # raise error
    if len(np.unique(dest)) != len(dest):
        pass  # raise error
    return dest


# apply, aggregation, where, check
###
def parse_fct(elem, key_word):
    f_ = []
    for f in elem[key_word].split(" "):
        f_ += [yetl.get_fct("steps", f)]
    return f_


def parse_to(elem):
    src = elem["to"].split(" ")
    if len(np.unique(src)) != len(src):
        pass  # raise error
    return src


# aggregation
###
def parse_as_aggregation(elem):
    fct = elem["aggregation"].split(" ")
    src = elem["to"].split(" ")
    dest = elem["as"].split(" ")
    if len(fct) * len(src) != len(dest):
        pass  # raise error
    return dest


# groupby
###
def parse_groupby(elem):
    src = elem["groupby"].split(" ")
    if len(src) != 1:
        pass  # raise error
    return elem["groupby"]


type_elem = {
    "select": {
        "select": parse_select,
        "as": lambda x: parse_as(x, "select")
    },
    "where": {
        "where": lambda x: parse_fct(x, "where"),
        "to": parse_to
    },
    "check": {
        "check": lambda x: parse_fct(x, "check"),
        "to": parse_to
    },
    "apply": {
        "apply": lambda x: parse_fct(x, "apply"),
        "to": parse_to,
        "as": lambda x: parse_as(x, "apply")
    },
    "aggregation": {
        "aggregation": lambda x: parse_fct(x, "aggregation"),
        "to": parse_to,
        "as": parse_as_aggregation,
    },
    "groupby": {
        "groupby": parse_groupby,
    },
}


def parse_elem(elem, parse_fct):
    ret = {}
    for k in elem:
        if k not in parse_fct:
            pass  # raise error
        ret[k] = parse_fct[k](elem)
    return ret


def resolve_steps(block):
    ret = []
    for elem in block:
        key = list(elem)[0]
        if key not in type_elem:
            pass  # raise error
        ret += [parse_elem(elem, type_elem[key])]
    return ret
