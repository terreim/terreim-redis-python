import socket  # noqa: F401
import selectors
from datetime import datetime, timedelta

from .exception import IncompleteData
from .resp import Decoder, Encoder

sel = selectors.DefaultSelector()
data_buffer = {}

def accept(sock: socket.socket, mask: int) -> None:
    conn, addr = sock.accept()

    print(f"Accepted connection from {addr}")

    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)


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
            decoder = Decoder(buf, 0)
            element, consumed = decoder.decode_resp()
            print(f"Received element: {element}, consumed {consumed} bytes")
        except IncompleteData:
            break

        del buf[:consumed]
        
        encoder = Encoder(element)
        response = encoder.encode_resp()
        conn.sendall(response)

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
