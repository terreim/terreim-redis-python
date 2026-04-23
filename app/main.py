import socket  # noqa: F401
import selectors

sel = selectors.DefaultSelector()

def accept(sock: socket.socket, mask: int) -> None:
    conn, addr = sock.accept()
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn: socket.socket, mask: int) -> None:
    data = conn.recv(1024)
    if data:
        print(f"Received data: {data}")
        response = b"+PONG\r\n"
        conn.sendall(response)
    else:
        print("Closing connection")
        sel.unregister(conn)
        conn.close()

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
