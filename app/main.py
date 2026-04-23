import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept()

    data = conn.recv(1024)
    if not data:
        return

    print(f"Received data: {data}")
    response = b"+PONG\r\n"
    conn.sendall(response)

    conn.close()

if __name__ == "__main__":
    main()
