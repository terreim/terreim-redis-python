from .exception import IncompleteData

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
    def encode_simple_string(s: bytes) -> bytes:
        return b'+' + s + b'\r\n'

    def encode_bulk_string(s: bytes | None) -> bytes:
        if s is None:
            return b'$-1\r\n'
        return b'$' + str(len(s)).encode() + b'\r\n' + s + b'\r\n'

    def encode_array(encoded_items: list[bytes]) -> bytes: # The caller does the encoding
        return b'*' + str(len(encoded_items)).encode() + b'\r\n' + b''.join(encoded_items)

    def encode_integer(n: int) -> bytes:
        return b':' + str(n).encode() + b'\r\n'

    def encode_error(msg: bytes) -> bytes:
        return b'-' + msg + b'\r\n'

