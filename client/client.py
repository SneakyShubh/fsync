import socket
import threading
import time
import os
import json
import hashlib
import zlib
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

BLOCK_SIZE = 6
WATCH_DIR = './syn'
BACKUP_DIR = './backup'

Path(BACKUP_DIR).mkdir(exist_ok=True)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("172.20.4.172", 12345))

print(client.recv(1024).decode())
username = input()
client.send(username.encode())

cnt = {}

def safe_open_for_write(path, mode='r+b', retries=5, delay=0.2):
    for attempt in range(retries):
        try:
            return open(path, mode)
        except PermissionError:
            if attempt == retries - 1:
                raise
            time.sleep(delay)

# --- Helper to get backup path ---
def get_backup_path(file_path):
    relative_path = file_path.relative_to(WATCH_DIR)
    backup_name = relative_path.stem + "_backup" + relative_path.suffix
    return Path(BACKUP_DIR) / backup_name

# --- File block utils ---
def get_file_blocks(filepath):
    blocks = []
    with open(filepath, 'rb') as f:
        while chunk := f.read(BLOCK_SIZE):
            weak = zlib.adler32(chunk)
            strong = hashlib.sha1(chunk).hexdigest()
            blocks.append({'weak': weak, 'strong': strong})
    return blocks

def build_delta(newfile_path, block_map):
    delta = []
    with open(newfile_path, 'rb') as f:
        index = 0
        while chunk := f.read(BLOCK_SIZE):
            weak = zlib.adler32(chunk)
            strong = hashlib.sha1(chunk).hexdigest()
            if not (index < len(block_map) and block_map[index]['weak'] == weak and block_map[index]['strong'] == strong):
                delta.append({
                    'filename': str(newfile_path),
                    'type': 'data',
                    'index': index,
                    'data': chunk.hex()
                })
            index += 1
    delta.append({'filename': str(newfile_path), 'type': 'truncate', 'length': os.path.getsize(newfile_path)})
    return delta

# --- Delta application logic ---
def apply_delta_from_bytes(delta):
    for entry in delta:
        file_path = entry["filename"]
        path_obj = Path(file_path)
        if file_path in cnt:
            cnt[file_path] += 1
        else:
            cnt[file_path] = 1

        if entry['type'] == 'data':
            with safe_open_for_write(file_path, 'r+b') as out:
                out.seek(entry["index"] * BLOCK_SIZE)
                out.write(bytes.fromhex(entry['data']))

        elif entry['type'] == 'truncate':
            with safe_open_for_write(file_path, 'r+b') as out:
                out.truncate(entry['length'])

        elif entry['type'] == 'create':
            Path(file_path).write_bytes(b'')
            get_backup_path(Path(file_path)).write_bytes(b'')

        elif entry['type'] == 'delete':
            if os.path.exists(file_path):
                os.remove(file_path)
            backup = get_backup_path(Path(file_path))
            if os.path.exists(backup):
                os.remove(backup)

# --- Receive thread ---
def receive_messages():
    while True:
        try:
            data = client.recv(8192).decode('utf-8')
            if not data:
                break
            delta = json.loads(data)
            apply_delta_from_bytes(delta)
        except Exception as e:
            print(f"Receive error: {e}")

# --- Watchdog Event Handler ---
class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        try:
            file_path = Path(event.src_path)
            if not file_path.is_file():
                return
            if file_path.name.endswith("_backup" + file_path.suffix):
                return

            if file_path in cnt and cnt[file_path]:
                cnt[file_path] -= 1
                return

            backup_path = get_backup_path(file_path)

            if not backup_path.exists():
                backup_path.write_bytes(b'')

            block_map = get_file_blocks(backup_path)
            delta = build_delta(file_path, block_map)
            delta_json = json.dumps(delta).encode('utf-8')
            client.send(delta_json)

            for entry in delta:
                if entry['type'] == 'data':
                    with safe_open_for_write(backup_path, 'r+b') as out:
                        out.seek(entry['index'] * BLOCK_SIZE)
                        out.write(bytes.fromhex(entry['data']))
                elif entry['type'] == 'truncate':
                    with safe_open_for_write(backup_path, 'r+b') as out:
                        out.truncate(entry['length'])

        except Exception as e:
            print(f"on_modified error: {e}")

    def on_created(self, event):
        try:
            file_path = Path(event.src_path)
            if not file_path.is_file():
                return
            if file_path.name.endswith("_backup" + file_path.suffix):
                return
            if file_path.suffix:
                backup_path = get_backup_path(file_path)
                file_path.write_bytes(b'')
                backup_path.write_bytes(b'')
                delta = [{'filename': str(file_path), 'type': 'create'}]
                client.send(json.dumps(delta).encode('utf-8'))
        except Exception as e:
            print(f"on_created error: {e}")

    def on_deleted(self, event):
        try:
            file_path = Path(event.src_path)
            if not file_path.is_file():
                return
            if file_path.name.endswith("_backup" + file_path.suffix):
                return
            delta = [{'filename': str(file_path), 'type': 'delete'}]
            client.send(json.dumps(delta).encode('utf-8'))
        except Exception as e:
            print(f"on_deleted error: {e}")

# --- Init everything ---
if __name__ == "__main__":
    threading.Thread(target=receive_messages, daemon=True).start()
    observer = Observer()
    observer.schedule(MyHandler(), path=WATCH_DIR, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
