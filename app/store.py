from dataclasses import dataclass
from datetime import datetime

from .exception import WrongTypeError

_keyspace: dict[bytes, StringValue | ListValue ] = {}

@dataclass
class StringValue:
    data: bytes
    expires_at: datetime | None

@dataclass
class ListValue:
    data: list[bytes]
    expires_at: datetime | None

def _check_alive(key: bytes) -> StringValue | ListValue:
    if key not in _keyspace:
        return None
    value = _keyspace[key]
    if value.expires_at is not None and value.expires_at < datetime.now():
        del _keyspace[key]
        return None
    return value
    
def get_string(key: bytes) -> bytes | None:
    v = _check_alive(key)
    if v is None: return None
    if not isinstance(v, StringValue): raise WrongTypeError(f"Key {key} is not a string")
    return v.data

def set_string(key, value, expiry=None) -> None:
    _keyspace[key] = StringValue(data=value, expires_at=datetime.now() + expiry if expiry is not None else None)

def rpush(key, *values) -> int:
    v = _check_alive(key)
    if v is None:
        _keyspace[key] = ListValue(data=list(values), expires_at=None)
    elif isinstance(v, ListValue):
        v.data.extend(values)
    else:
        raise WrongTypeError(f"Key {key} is not a list")
    return len(_keyspace[key].data)

def lpush(key, *values) -> int:
    v = _check_alive(key)
    if v is None:
        _keyspace[key] = ListValue(data=list(values), expires_at=None)
    elif isinstance(v, ListValue):
        v.data[0:0] = values[::-1]
    else:
        raise WrongTypeError(f"Key {key} is not a list")
    return len(_keyspace[key].data)

def lrange(key, start, stop) -> list[bytes]:
    v = _check_alive(key)
    if v is None: return []
    if not isinstance(v, ListValue): raise WrongTypeError(f"Key {key} is not a list")
    
    return v.data[start:stop]

def lpop(key, count=1) -> bytes | list[bytes] | None:
    v = _check_alive(key)
    if v is None or len(v.data) == 0: return None
    if not isinstance(v, ListValue): raise WrongTypeError(f"Key {key} is not a list")
    
    if count == 1:
        return v.data.pop(0)
    else:
        popped = v.data[:count]
        del v.data[:count]
        return popped

def llen(key) -> int:
    v = _check_alive(key)
    if v is None: return 0
    if not isinstance(v, ListValue): raise WrongTypeError(f"Key {key} is not a list")
    return len(v.data)