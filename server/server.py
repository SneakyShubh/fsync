import socket
import threading
import json

BLOCK_SIZE = 6

def load_original_blocks(filepath):
    blocks = []
    with open(filepath, 'rb') as f:
        while chunk := f.read(BLOCK_SIZE):
            blocks.append(chunk)
    return blocks

def apply_delta_from_bytes(delta):

        for entry in delta:
            with open(entry["filename"], 'r+b') as out:
                out.seek(entry["index"] * BLOCK_SIZE)
                out.write(bytes.fromhex(entry['data']))

clients = {}  

def handle_client(client, username):
    try:
        while True:
            delta_bytes = b''
            while True:
                part = client.recv(4096)
                if not part:
                    break
                delta_bytes += part
                if len(part) < 4096:
                    break
            for other_user, other_client in clients.items():
                if other_user != username:
                    other_client.sendall(delta_bytes)

    except Exception as e:
        print(f"error: {e}")

    print(f"{username} disconnected")
    del clients[username]
    client.close()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("0.0.0.0", 12345))
server.listen(5)
print("Server is listening...")

while True:
    client, addr = server.accept()
    client.send("Enter your username: ".encode())
    username = client.recv(1024).decode()
    clients[username] = client
    print(f"{username} connected from {addr}")
    threading.Thread(target=handle_client, args=(client, username)).start()
