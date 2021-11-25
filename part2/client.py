import os
import socket
import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, LoggingEventHandler

CREATE_COMMAND = 1
DELETE_COMMAND = 2
MODIFY_COMMAND = 3
PULL_COMMAND = 4


class ClientDisconnectedException(BaseException):
    def __init__(self):
        super().__init__(self, "Client Disconnected")


def updates_from_server(identifier, s, base_path):
    is_identifier = 1
    is_identifier = is_identifier.to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    pull = PULL_COMMAND.to_bytes(1, 'little')
    data = is_identifier + identifier + pull
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
        updates_from_server(identifier, s, path)
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
            updates_from_server(identifier, s, path)
    except (KeyboardInterrupt, ClientDisconnectedException):
        observer.stop()
    observer.join()
