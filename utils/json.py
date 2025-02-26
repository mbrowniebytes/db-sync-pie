import decimal

import orjson


# TODO make a class
# class Json:


# json does not serialize DateTime
# orjson, faster, does not serialize Decimal
# https://github.com/ijl/orjson
def orjson_default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError


def json_dumps(o):
    return orjson.dumps(o, default=orjson_default)
