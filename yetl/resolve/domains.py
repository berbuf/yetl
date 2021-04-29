import psycopg2
from yetl import yetl


# options
####
def parse_options(elem):
    # check validity
    return elem["options"]


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


# postgres
####
def parse_postgres(elem):
    return ""


# move to connectors
def execute_psql(connection, cmd):
    with psycopg2.connect(connection) as conn:
        with conn.cursor() as curs:
            curs.execute(cmd)
            return curs.fetchall()


def parse_connection(elem):
    # check if connection works
    try:
        psycopg2.connect(elem["connection"])
    except Exception as e:
        print(e)
        pass  # raise error
    # check if table exists
    if not elem["create"]:
        try:
            cmd = "SELECT 1 FROM {}".format(elem["table"])
            res = execute_psql(elem["connection"], cmd)
        except Exception as e:
            print(e)
            pass  # raise error

    return elem["connection"]


def parse_table(elem):
    # check integirty ?
    return elem["table"]


def parse_conflict_keys(elem):
    return elem["conflict_keys"].split(" ")


# name
####
def parse_name(elem):
    if "from" in elem:
        return (elem["from"], elem["name"])
    # check validity
    # verify existence
    return (elem["name"], elem["name"])


def parse_domain(elem):
    f_ = []
    for f in elem["domain"].split(" "):
        f_ += [yetl.get_fct("domains", f)]
    if "in" in elem:
        f_ += [lambda x: x in elem["in"].split(" ")]
    return f_


def parse_from(elem):
    # verify existence
    return elem["from"]


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


# options
####
def parse_options_in(elem):
    return elem["options"]


def parse_options_out(elem):
    return elem["options"]


options_ = {
    "in": parse_options_in,
    "out": parse_options_out
}


type_elem = {
    "options": {
        "options": (),
    },
    "path": {
        "path": parse_path,
        "format": parse_format,
        "sep": parse_sep,
    },
    "postgres": {
        "postgres": parse_postgres,
        "connection": parse_connection,
        "table": parse_table,
        "create": lambda x: x,
        "conflict_keys": parse_conflict_keys,
    },
    "name": {
        "name": parse_name,
        "domain": parse_domain,
        "in": parse_in,
        "from": parse_from,
        "comment": parse_comment,
    }
}


# pre checks
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


def check_has_one_of(block, values):
    pass


def pre_checks(block):
    check_same_names(block)
    check_has_one_of(block, ["path", "postgres"])
    check_has(block, "path", 1)
    # check from and name does not overlap


def parse_elem(elem, parse_fct):
    ret = {}
    for k in elem:
        if k not in parse_fct:
            pass  # raise error
        ret[k] = parse_fct[k](elem)
    return ret


def resolve_domains(block, in_out):
    type_elem["options"]["options"] = options_[in_out]
    pre_checks(block)
    ret = []
    for elem in block:
        key = list(elem)[0]
        if key not in type_elem:
            pass  # raise error
        ret += [parse_elem(elem, type_elem[key])]
    return ret
