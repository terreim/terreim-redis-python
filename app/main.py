import socket  # noqa: F401
import selectors

from .exception import IncompleteData

sel = selectors.DefaultSelector()
data_buffer = {}
get_dict = {
    b'foo': b'bar'
}

def accept(sock: socket.socket, mask: int) -> None:
    conn, addr = sock.accept()

    print(f"Accepted connection from {addr}")

    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

# Parse RESP data
def parse_element(buf: bytes, pos: int) -> tuple[bytes | list[bytes], int]:

    if pos >= len(buf): raise IncompleteData
    prefix = buf[pos:pos + 1]
    
    match prefix:
        case b'*': return _handle_array(buf, pos)
        case b'$': return _handle_bulk_string(buf, pos)
        case b'+': return _handle_simple_string(buf, pos)
        case b':': return _handle_integer(buf, pos)
        case b'-': return _handle_error(buf, pos)
        case _: raise ValueError(f"Unknown RESP type: {prefix}")

def _handle_array(buf: bytes, pos: int) -> tuple[list[bytes], int]:
    line_end = buf.find(b'\r\n', pos)
    if line_end == -1: raise IncompleteData

    count = int(buf[pos + 1:line_end])
    pos = line_end + 2 # Skip past the \r\n

    elements = []
    for _ in range(count):
        child, pos = parse_element(buf, pos)
        elements.append(child)

    return elements, pos
    
def _handle_bulk_string(buf: bytes, pos: int) -> tuple[bytes, int]:
    line_end = buf.find(b'\r\n', pos)
    if line_end == -1: raise IncompleteData

    length = int(buf[pos + 1:line_end])
    start = line_end + 2
    end = start + length

    if len(buf) < end + 2: raise IncompleteData

    return bytes(buf[start:end]), end + 2

def _handle_simple_string(buf: bytes, pos: int) -> tuple[bytes, int]:
    line_end = buf.find(b'\r\n', pos)
    if line_end == -1: raise IncompleteData

    return bytes(buf[pos + 1:line_end]), line_end + 2

def _handle_integer(buf: bytes, pos: int) -> tuple[bytes, int]:
    line_end = buf.find(b'\r\n', pos)
    if line_end == -1: raise IncompleteData

    return bytes(buf[pos + 1:line_end]), line_end + 2

def _handle_error(buf: bytes, pos: int) -> tuple[bytes, int]:
    line_end = buf.find(b'\r\n', pos)
    if line_end == -1: raise IncompleteData

    return bytes(buf[pos + 1:line_end]), line_end + 2


def read(conn: socket.socket, mask: int) -> None:
    chunk = conn.recv(1024)

    if not chunk:
        sel.unregister(conn)
        conn.close()
        data_buffer.pop(conn, None)
        return
    
    buf = data_buffer.setdefault(conn, bytearray())
    buf.extend(chunk)

    while len(buf) > 0:
        try:
            element, consumed = parse_element(buf, 0)
            print(f"Received element: {element}, consumed {consumed} bytes")
        except IncompleteData:
            break

        del buf[:consumed]
        match element:
            case [b'PING']:
                conn.sendall(b'+PONG\r\n')
            case [b'ECHO', msg]:
                conn.sendall(b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n")
            case [b'SET', key, value]:
                get_dict[key] = value
                conn.sendall(b'+OK\r\n')
            case [b'GET', key]:
                if key in get_dict:
                    conn.sendall(b'$' + str(len(get_dict[key])).encode() + b'\r\n' + get_dict[key] + b'\r\n')
                else:
                    conn.sendall(b'$-1\r\n')
            case _:
                conn.sendall(b'-ERR unknown command\r\n')

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, accept)

    try:
        while True:
            events = sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
    except KeyboardInterrupt:
        print("Shutting down server...")
        server_socket.close()

if __name__ == "__main__":
    main()
