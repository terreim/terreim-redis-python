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

        case [b'SET', key, value, *tail]:
            if not tail:
                set_string(key, value)
                return encode_simple_string(b'OK')

            if len(tail) == 2:
                option, rest = tail
                option = option.upper()
                if option == b'EX':
                    set_string(key, value, expiry=timedelta(seconds=int(rest)))
                    return encode_simple_string(b'OK')
                if option == b'PX':
                    set_string(key, value, expiry=timedelta(milliseconds=int(rest)))
                    return encode_simple_string(b'OK')

            return encode_error(b'ERR syntax error')
            
        case [b'GET', key]:
            value = get_string(key)
            if value is None:
                return encode_bulk_string(None)
            return encode_bulk_string(value)
        
        case [b'RPUSH', key, *value]:
            return encode_integer(rpush(key, *value))
        
        case [b'LPUSH', key, *value]:
            return encode_integer(lpush(key, *value))
        
        case [b'LPOP', key, *tail]:
            if tail > 1:
                return encode_error(b'ERR syntax error')
            
            cnt = 1 if len(tail) == 0 else int(tail[0])
            popped = lpop(key, count=cnt)
            if popped is None:
                return encode_bulk_string(None)
            elif isinstance(popped, list):
                return encode_array([encode_bulk_string(v) for v in popped])
            else:
                return encode_bulk_string(popped)
            
        
        case [b'LRANGE', key, start, stop]:
            start_idx = int(start)
            stop_idx = int(stop)
            list_len = llen(key)

            if stop_idx < 0:
                if stop_idx == -1: stop_idx = list_len
                else: stop_idx = list_len + stop_idx + 1
            else:
                stop_idx += 1 # Why is stop_idx inclusive?

            if start_idx >= list_len or start_idx >= stop_idx:
                return encode_array([])
            
            return encode_array([encode_bulk_string(v) for v in lrange(key, start_idx, stop_idx)])
        
        case [b'LLEN', key]:
            return encode_integer(llen(key))
        
        case _:
            return encode_error(b'ERR unknown command')
        