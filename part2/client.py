import os
import socket
import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, LoggingEventHandler


# data = s.recv()
# print("Server sent: ", data)
# s.close()

def updates_from_server(identifier, s):
    is_identifier = 1
    is_identifier = is_identifier.to_bytes(1, 'little')
    identifier = bytes(identifier)
    pull = 4
    pull = pull.to_bytes(1, 'little')
    data = is_identifier + identifier + pull
    s.send(data)
    while True:
        path_size = int(s.recv(4))
        if path_size == 0:
            break

        path = str(s.recv(path_size))
        file_size = int(s.recv(4))
        file_data = s.recv(file_size)
        with open(path, 'wb+') as f:
            f.write(file_data)


def push_file_to_server(identifier, s, file_path):
    is_identifier = int(1).to_bytes(1, 'little')
    identifier = bytes(identifier)
    create = int(1).to_bytes(1, 'little')
    size_path = len(file_path).to_bytes(4, 'little')
    with open(file_path, 'rb') as f:
        data = f.read()

    file_size = len(data).to_bytes(4, 'little')
    packet_to_send = is_identifier + identifier + create + size_path + bytes(file_path) + file_size + data
    s.send(packet_to_send)


def push_all_to_server(identifier, s, path):
    for root, subdirs, files in os.walk(path):
        for file in files:
            push_file_to_server(identifier, s, file)
        for subdir in subdirs:
            push_all_to_server(identifier, s, subdir)


def first_connected_to_server(identifier, s, path):
    if identifier:
        updates_from_server(identifier, s)
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
    return s.recv(128)


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
    path = sys.argv[3]
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
    first_connected_to_server(identifier, s, path)
    s.close()

    try:
        while True:
            # Set the thread sleep time
            time.sleep(time_series)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, port_num))
            updates_from_server(identifier, s)
            s.close()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
