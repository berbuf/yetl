from yetl import yetl


# path
####
def parse_path(elem):
    # check validity
    return elem["path"]


def parse_format(elem):
    if elem["format"] == "csv":
        # check if file exists
        # check if url is correct
        pass
    return elem["format"]


def parse_sep(elem):
    # check validity
    return elem["sep"]


# col
####
def parse_name(elem):
    # check validity
    return elem["name"]


def parse_domain(elem):
    f_ = []
    for f in elem["domain"].split(" "):
        f_ += [yetl.get_fct("domains", f)]
    if "in" in elem:
        f_ += [lambda x: x in elem["in"].split(" ")]
    return f_


def parse_in(elem):
    if "domain" not in elem:
        pass  # raise error
    type_ = elem["domain"].split(" ")[0]
    if type_ not in ["str"]:  # only str for now
        pass  # raise error
    return None


def parse_comment(elem):
    # check validity
    return elem["comment"]


# file in
####
def check_same_names(block):
    names = []
    for e in block:
        if "name" not in e:
            continue
        if e["name"] in names:
            pass  # raise error
        names += [e["name"]]


def check_has(block, name, n):
    ret = [1 for e in block if name in e]
    if len(ret) != n or not len(ret):
        pass  # raise error


type_elem = {
    "path": {
        "path": parse_path,
        "format": parse_format,
        "sep": parse_sep,
    },
    "name": {
        "name": parse_name,
        "domain": parse_domain,
        "in": parse_in,
        "comment": parse_comment,
    }
}


def parse_elem(elem, parse_fct):
    ret = {}
    for k in elem:
        if k not in parse_fct:
            pass  # raise error
        ret[k] = parse_fct[k](elem)
    return ret


def resolve_in(block):
    check_same_names(block)
    check_has(block, "path", 1)
    ret = []
    for elem in block:
        key = list(elem)[0]
        if key not in type_elem:
            pass  # raise error
        ret += [parse_elem(elem, type_elem[key])]
    return ret
