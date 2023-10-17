def agg_func1(tuples):
    sum = 0
    for tuple in tuples:
        sum += int(tuple["__result__.prpr_map.amount"])
    return str(sum)

def where1(tuple):
    return tuple["__result__.products.name"] == "Sugar"


def agg_func2(tuples):
    sum = 0
    for tuple in tuples:
        sum += int(tuple['__result__.products.price']) * int(tuple['__result__.prpr_map.amount'])
    return str(sum)

def where2(tuple):
    return tuple["__result__.procurements.company"] == "Lenta"


def agg_func3(tuples):
    mx = 0
    for tuple in tuples:
        mx = max(mx, int(tuple['__result__.prpr_map.amount']))
    return str(mx)

def where3(tuple):
    return tuple["__result__.procurements.company"] == "Perek"


def agg_func4(tuples):
    mx = 0
    for tuple in tuples:
        mx = max(mx, int(tuple['products.price']))
    return str(mx)

def where4(tuple):
    return True


def where5(tuple):
    return True