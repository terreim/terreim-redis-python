from .exception import IncompleteData
from .store import sg_dict, RedisValue
from datetime import datetime, timedelta

class Decoder():
    def __init__(self, buf: bytes, pos: int):
        self.buffer = buf
        self.pos = pos
    
    def decode_resp(self) -> bytes:
        if self.pos >= len(self.buffer):
            raise IncompleteData
        
        type_byte = self.buffer[self.pos:self.pos + 1]
        if type_byte == b'*':
            return self._decode_array()
        elif type_byte == b'$':
            return self._decode_bulk_string()
        elif type_byte == b'+':
            return self._decode_simple_string()
        elif type_byte == b':':
            return self._decode_integer()
        elif type_byte == b'-':
            return self._decode_error()
        else:
            raise ValueError(f"Unknown RESP type byte: {type_byte}")

    def _decode_array(self) -> tuple[list[bytes], int]:
        line_end = self.buffer.find(b'\r\n', self.pos)
        if line_end == -1: raise IncompleteData

        count = int(self.buffer[self.pos + 1:line_end])
        self.pos = line_end + 2 # Skip past the \r\n

        elements = []
        for _ in range(count):
            child, self.pos = self.decode_resp()
            elements.append(child)

        return elements, self.pos

    def _decode_bulk_string(self) -> tuple[bytes, int]:
        line_end = self.buffer.find(b'\r\n', self.pos)
        if line_end == -1: raise IncompleteData

        length = int(self.buffer[self.pos + 1:line_end])
        start = line_end + 2
        end = start + length

        if len(self.buffer) < end + 2: raise IncompleteData

        return bytes(self.buffer[start:end]), end + 2

    def _decode_simple_string(self) -> tuple[bytes, int]:
        line_end = self.buffer.find(b'\r\n', self.pos)
        if line_end == -1: raise IncompleteData

        return bytes(self.buffer[self.pos + 1:line_end]), line_end + 2

    def _decode_integer(self) -> tuple[bytes, int]:
        line_end = self.buffer.find(b'\r\n', self.pos)
        if line_end == -1: raise IncompleteData

        return bytes(self.buffer[self.pos + 1:line_end]), line_end + 2

    def _decode_error(self) -> tuple[bytes, int]:
        line_end = self.buffer.find(b'\r\n', self.pos)
        if line_end == -1: raise IncompleteData

        return bytes(self.buffer[self.pos + 1:line_end]), line_end + 2
    
class Encoder:
    def __init__(self, element):
        self.element = element

    def encode_resp(self) -> bytes:
        match self.element:
            case [b'PING']:
                return b'+PONG\r\n'

            case [b'ECHO', msg]:
                return self._encode_bulk_string(msg)

            case [b'SET', key, value]:
                sg_dict[key] = RedisValue(data=value, expires_at=None)
                return b'+OK\r\n'

            case [b'SET', key, value, options, rest]:
                sg_dict[key] = RedisValue(data=value, expires_at=datetime.now() + timedelta(seconds=int(rest)) if options.upper() == b'EX' else datetime.now() + timedelta(milliseconds=int(rest)))
                return b'+OK\r\n'
                
            case [b'GET', key]:
                if key in sg_dict:
                    redis_value = sg_dict[key]
                    if redis_value.expires_at is None or redis_value.expires_at > datetime.now():
                        return self._encode_bulk_string(redis_value.data)
                    del sg_dict[key]
                return self._encode_bulk_string(None)
            
            case [b'RPUSH', list, value]:
                if list not in sg_dict:
                    sg_dict[list] = RedisValue(data=[], expires_at=None)
                sg_dict[list].data.append(value)
                return self._encode_integer(len(sg_dict[list].data))
            
            case _:
                return self._encode_error(b'ERR unknown command')
            
    def _encode_simple_string(self, s: bytes) -> bytes:
        return b'+' + s + b'\r\n'

    def _encode_bulk_string(self, s: bytes | None) -> bytes:  # None → b"$-1\r\n"
        if s is None:
            return b'$-1\r\n'
        return b'$' + str(len(s)).encode() + b'\r\n' + s + b'\r\n'

    def _encode_array(self, items: list[bytes]) -> bytes:
        pass

    def _encode_integer(self, n: int) -> bytes:
        return b':' + str(n).encode() + b'\r\n'

    def _encode_error(self, msg: bytes) -> bytes:
        return b'-' + msg + b'\r\n'

