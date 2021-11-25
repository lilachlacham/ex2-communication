import os
import socket
import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, LoggingEventHandler

# Client commands
CREATE_COMMAND = 1
DELETE_COMMAND = 2
MODIFY_COMMAND = 3
MOVE_COMMAND = 4
PULL_COMMAND = 5
UPDATES_COMMAND = 6


class ClientDisconnectedException(BaseException):
    def __init__(self):
        super().__init__(self, "Client Disconnected")


def pull_updates_from_server(identifier, s, base_path):
    is_identifier = 1
    is_identifier = is_identifier.to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    updates = UPDATES_COMMAND.to_bytes(1, 'little')
    data = is_identifier + identifier + updates
    s.send(data)
    while True:
        command = s.recv(1)
        if not command:
            raise ClientDisconnectedException()
        command = int.from_bytes(command, 'utf-8')
        if command != CREATE_COMMAND:
            break
        is_directory = int.from_bytes(s.recv(1), 'little')
        path_size = int.from_bytes(s.recv(4), 'utf-8')
        path = os.path.join(base_path, s.recv(path_size).decode('utf-8'))
        if is_directory:
            os.makedirs(path, exist_ok=True)
            continue
        file_size = int.from_bytes(s.recv(4), 'utf-8')
        file_data = s.recv(file_size)
        with open(path, 'wb+') as f:
            f.write(file_data)


def delete_recursive(path):
    for root, subdirs, files in os.walk(path, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for subdir in subdirs:
            os.remove(os.path.join(root, subdir))


def handle_command_from_server(command, is_directory, path, base_path, s):
    if command == CREATE_COMMAND:
        if is_directory:
            os.makedirs(path, exist_ok=True)
            return
        file_size = int.from_bytes(s.recv(4), 'utf-8')
        file_data = s.recv(file_size)
        with open(path, 'wb+') as f:
            f.write(file_data)
    elif command == DELETE_COMMAND:
        if not is_directory:
            os.remove(path)
        else:
            delete_recursive(path)
    elif command == MODIFY_COMMAND:
        file_size = int.from_bytes(s.recv(4), 'utf-8')
        file_data = s.recv(file_size)
        with open(path, 'wb+') as f:
            f.write(file_data)
    elif command == MOVE_COMMAND:
        dest_path_size = int.from_bytes(s.recv(4), 'utf-8')
        dest_path = os.path.join(base_path, s.recv(dest_path_size).decode('utf-8'))
        if not is_directory and os.path.isfile(dest_path):
            os.remove(dest_path)
        elif is_directory and os.path.isdir(dest_path):
            delete_recursive(dest_path)
        os.rename(path, dest_path)


def pull_all_from_server(identifier, s, base_path):
    is_identifier = 1
    is_identifier = is_identifier.to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    pull = PULL_COMMAND.to_bytes(1, 'little')
    data = is_identifier + identifier + pull
    s.send(data)

    counts = s.recv(4)
    if not counts:
        raise ClientDisconnectedException()

    counts = int.from_bytes(counts, 'little')
    for _ in range(counts):
        command = int.from_bytes(s.recv(1), 'utf-8')
        is_directory = int.from_bytes(s.recv(1), 'little')
        path_size = int.from_bytes(s.recv(4), 'utf-8')
        path = os.path.join(base_path, s.recv(path_size).decode('utf-8'))
        handle_command_from_server(command, is_directory, path, base_path, s)


def push_file_to_server(identifier, s, file_path, base_path):
    is_identifier = int(1).to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    create = CREATE_COMMAND.to_bytes(1, 'little')
    # Append listening directory name with file path
    rel_file_path = os.path.relpath(file_path, base_path)
    path_size = len(rel_file_path).to_bytes(4, 'little')
    is_directory = os.path.isdir(file_path).to_bytes(1, 'little')
    if os.path.isdir(file_path):
        packet_to_send = is_identifier + identifier + create + is_directory + path_size + rel_file_path.encode('utf-8')
    else:
        with open(file_path, 'rb') as f:
            data = f.read()

        file_size = len(data).to_bytes(4, 'little')
        packet_to_send = is_identifier + identifier + create + is_directory + path_size + rel_file_path.encode(
            'utf-8') + file_size + data
    s.send(packet_to_send)


def push_all_to_server(identifier, s, path):
    for root, subdirs, files in os.walk(path):
        for file in files:
            push_file_to_server(identifier, s, os.path.join(root, file), path)
        for subdir in subdirs:
            if not os.listdir(os.path.join(root, subdir)):
                push_file_to_server(identifier, s, os.path.join(root, subdir), path)


def first_connected_to_server(identifier, s, path):
    if identifier:
        pull_all_from_server(identifier, s, path)
        return identifier
    else:
        identifier = get_identifier_from_server(s)
        push_all_to_server(identifier, s, path)
        return identifier


def get_identifier_from_server(s):
    is_identifier = 0
    is_identifier = is_identifier.to_bytes(1, 'little')
    data = is_identifier
    s.send(data)
    return s.recv(128).decode('utf-8')


class Handler(PatternMatchingEventHandler):

    def __init__(self):
        super(Handler, self).__init__(ignore_patterns=['*.goutputstream*'])

    def on_created(self, event):
        print(f"Created {event.src_path}, is directory: {event.is_directory}")

    def on_deleted(self, event):
        print(f"Deleted {event.src_path}, is directory: {event.is_directory}")

    def on_modified(self, event):
        pass

    def on_moved(self, event):
        print(f"Moved from {event.src_path} to {event.dest_path}, is directory: {event.is_directory}")


if __name__ == "__main__":
    ip = sys.argv[1]
    port_num = int(sys.argv[2])
    path = os.path.abspath(sys.argv[3])
    time_series = int(sys.argv[4])
    if len(sys.argv) == 6:
        identifier = sys.argv[5]
    else:
        identifier = None

    # Initialize logging event handler
    event_handler = Handler()

    # Initialize Observer
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)

    # Start the observer
    observer.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port_num))
    identifier = first_connected_to_server(identifier, s, path)

    try:
        while True:
            # Set the thread sleep time
            time.sleep(time_series)
            pull_all_from_server(identifier, s, path)
    except (KeyboardInterrupt, ClientDisconnectedException):
        observer.stop()
    observer.join()
