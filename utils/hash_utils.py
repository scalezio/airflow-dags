from hashlib import blake2b


def hash_object(obj):
    if isinstance(obj, dict):
        return hash_primitive(','.join(sorted([f'{k}_{hash_object(v)}' for k, v in obj.items()])))
    if isinstance(obj, list):
        return hash_primitive(','.join((sorted([hash_object(v) for v in obj]))))
    else:
        return hash_primitive(obj)


def hash_primitive(s):
    return str(blake2b(str.encode(str(s)), digest_size=10).hexdigest())
