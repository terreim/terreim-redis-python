from datetime import datetime, timedelta

from .resp import (
    encode_array,
    encode_bulk_string,
    encode_simple_string,
    encode_integer,
    encode_error
)
from .store import get_string, set_string, rpush, lpush, lrange, lpop, llen

def dispatch(command: list[bytes]) -> bytes:
    match command:
        case [b'PING']:
            return encode_simple_string(b'PONG')

        case [b'ECHO', msg]:
            return encode_bulk_string(msg)

        case [b'SET', key, value]:
            set_string(key, value)
            return encode_simple_string(b'OK')

        case [b'SET', key, value, option, rest]:
            if option.upper() == b'EX':
                set_string(key, value, expiry=timedelta(seconds=int(rest)))
            elif option.upper() == b'PX':
                set_string(key, value, expiry=timedelta(milliseconds=int(rest)))
            return encode_simple_string(b'OK')
            
        case [b'GET', key]:
            value = get_string(key)
            if value is None:
                return encode_bulk_string(None)
            return encode_bulk_string(value)
        
        case [b'RPUSH', key, *value]:
            return encode_integer(rpush(key, *value))
        
        case [b'LRANGE', key, start, stop]:
            start_idx = int(start)
            stop_idx = int(stop)
            list_len = llen(key)

            if stop_idx < 0:
                if stop_idx == -1: stop_idx = list_len
                else: stop_idx = list_len + stop_idx + 1

            if start_idx >= list_len or start_idx >= stop_idx:
                return encode_array([])
            
            return encode_array([encode_bulk_string(v) for v in lrange(key, start_idx, stop_idx)])
        
        case [b'LLEN', key]:
            return encode_integer(llen(key))
        
        case _:
            return encode_error(b'ERR unknown command')
        