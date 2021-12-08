import os
import socket
import sys
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# Client commands
CREATE_COMMAND = 1
DELETE_COMMAND = 2
MODIFY_COMMAND = 3
MOVE_COMMAND = 4
PULL_COMMAND = 5
UPDATES_COMMAND = 6

observer = None


# Start watchdog observer on base_path parameter
def start_watchdog(base_path, s, identifier):
    global observer
    if observer:
        return
    # Initialize logging event handler
    event_handler = Handler(base_path, s, identifier)

    # Initialize Observer
    observer = Observer()
    observer.schedule(event_handler, base_path, recursive=True)

    # Start the observer
    observer.start()


# Stop running watchdog observer
def stop_watchdog():
    global observer
    if observer:
        observer.stop()
        wait_observer()
        observer = None


# Wait for observer to exit
def wait_observer():
    global observer
    if observer:
        observer.join()


class ClientDisconnectedException(BaseException):
    def __init__(self):
        super().__init__(self, "Client Disconnected")


def recv(client_socket, recv_size):
    recv_data = b''
    while len(recv_data) < recv_size:
        recv_data += client_socket.recv(recv_size - len(recv_data))

    return recv_data


# First connection to server with identifier, we receive all directory
def pull_all_from_server(identifier, s, base_path):
    is_identifier = 1
    is_identifier = is_identifier.to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    updates = PULL_COMMAND.to_bytes(1, 'little')
    data = is_identifier + identifier + updates
    s.sendall(data)

    while True:
        command = s.recv(1)
        # If empty array it means the server exit
        if not command:
            raise ClientDisconnectedException()
        command = int.from_bytes(command, 'little', signed=True)
        # If command == -1 it means we send invalid identifier
        if command == -1:
            raise ClientDisconnectedException()
        # If we got the special packet that indicates there is no more files
        if command != CREATE_COMMAND:
            break
        is_directory = int.from_bytes(s.recv(1), 'little')
        path_size = int.from_bytes(s.recv(4), 'little')
        path = os.path.join(base_path, s.recv(path_size).decode('utf-8'))
        path = path.replace("/", os.sep)
        path = path.replace('\\', os.sep)
        if is_directory:
            os.makedirs(path, exist_ok=True)
            continue
        file_size = int.from_bytes(s.recv(4), 'little')
        file_data = recv(s, file_size)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb+') as f:
            f.write(file_data)


def delete_recursive(path):
    for root, subdirs, files in os.walk(path, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for subdir in subdirs:
            os.rmdir(os.path.join(root, subdir))
    if os.path.isdir(path):
        os.rmdir(path)


def handle_command_from_server(command, is_directory, path, base_path, s):
    if command == CREATE_COMMAND:
        if is_directory:
            os.makedirs(path, exist_ok=True)
            return
        file_size = int.from_bytes(s.recv(4), 'little')
        file_data = recv(s, file_size)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb+') as f:
            f.write(file_data)
    elif command == DELETE_COMMAND:
        if not os.path.isdir(path):
            if os.path.isfile(path):
                # If it file
                os.remove(path)
        else:
            delete_recursive(path)
    elif command == MODIFY_COMMAND:
        file_size = int.from_bytes(s.recv(4), 'little')
        file_data = recv(s, file_size)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb+') as f:
            f.write(file_data)
    elif command == MOVE_COMMAND:
        dst_path_size = int.from_bytes(s.recv(4), 'little')
        dst_path = os.path.join(base_path, s.recv(dst_path_size).decode('utf-8'))
        dst_path = dst_path.replace("/", os.sep)
        dst_path = dst_path.replace('\\', os.sep)

        # If is file and destination path exists we delete it
        if not is_directory and os.path.isfile(dst_path):
            os.remove(dst_path)

        os.makedirs(os.path.dirname(dst_path), exist_ok=True)

        # If path is dir and the source dir is empty dir, we delete it, otherwise rename the file
        if is_directory and os.path.isdir(dst_path):
            if not os.listdir(path):
                os.rmdir(path)
        else:
            os.rename(path, dst_path)


def pull_updates_from_server(identifier, s, base_path):
    is_identifier = 1
    is_identifier = is_identifier.to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    updates = UPDATES_COMMAND.to_bytes(1, 'little')
    data = is_identifier + identifier + updates
    s.sendall(data)

    counts = s.recv(4)
    if not counts:
        raise ClientDisconnectedException()

    counts = int.from_bytes(counts, 'little')
    for _ in range(counts):
        command = int.from_bytes(s.recv(1), 'little')
        is_directory = int.from_bytes(s.recv(1), 'little')
        path_size = int.from_bytes(s.recv(4), 'little')
        path = os.path.join(base_path, s.recv(path_size).decode('utf-8'))
        path = path.replace("/", os.sep)
        path = path.replace('\\', os.sep)
        handle_command_from_server(command, is_directory, path, base_path, s)


def push_file_to_server(identifier, s, file_path, base_path):
    is_identifier = int(1).to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    create = CREATE_COMMAND.to_bytes(1, 'little')
    # Append listening directory name with file path
    sent_file_path = os.path.relpath(file_path, base_path)
    path_size = len(sent_file_path).to_bytes(4, 'little')
    is_directory = os.path.isdir(file_path).to_bytes(1, 'little')
    if os.path.isdir(file_path):
        packet_to_send = is_identifier + identifier + create + is_directory + path_size + sent_file_path.encode('utf-8')
    else:
        # If the file is not exists we return
        if not os.path.isfile(file_path):
            return

        try:
            with open(file_path, 'rb') as f:
                data = f.read()
        except PermissionError:
            return

        file_size = len(data).to_bytes(4, 'little')
        packet_to_send = is_identifier + identifier + create + is_directory + path_size + sent_file_path.encode(
            'utf-8') + file_size + data
    s.sendall(packet_to_send)


def push_all_to_server(identifier, s, path):
    for root, subdirs, files in os.walk(path):
        for file in files:
            push_file_to_server(identifier, s, os.path.join(root, file), path)
        for subdir in subdirs:
            if not os.listdir(os.path.join(root, subdir)):
                push_file_to_server(identifier, s, os.path.join(root, subdir), path)


def first_connected_to_server(identifier, s, path):
    if identifier:
        # If we accept identifier from command line, we remove the local path directory and get all files from server
        delete_recursive(path)
        pull_all_from_server(identifier, s, path)
        return identifier
    else:
        # If we dont accepted identifier from command line, we got one from the server and push all files to server
        identifier = get_identifier_from_server(s)
        push_all_to_server(identifier, s, path)
        return identifier


def get_identifier_from_server(s):
    is_identifier = 0
    is_identifier = is_identifier.to_bytes(1, 'little')
    data = is_identifier
    s.sendall(data)
    return s.recv(128).decode('utf-8')


# Send create message for update
def send_create_message(client_socket, identifier, base_path, src_path, is_directory):
    push_file_to_server(identifier, client_socket, src_path, base_path)


# Send delete message for update
def send_delete_message(client_socket, identifier, base_path, file_path, is_directory):
    is_identifier = int(1).to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    delete = DELETE_COMMAND.to_bytes(1, 'little')
    # Append listening directory name with file path
    sent_file_path = os.path.relpath(file_path, base_path)
    path_size = len(sent_file_path).to_bytes(4, 'little')
    is_directory = is_directory.to_bytes(1, 'little')

    packet = is_identifier + identifier + delete + is_directory + path_size + sent_file_path.encode('utf-8')
    client_socket.sendall(packet)


def send_modify_message(client_socket, identifier, base_path, file_path, is_directory):
    is_identifier = int(1).to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    modify = MODIFY_COMMAND.to_bytes(1, 'little')
    # Append listening directory name with file path
    sent_file_path = os.path.relpath(file_path, base_path)
    path_size = len(sent_file_path).to_bytes(4, 'little')
    is_directory = is_directory.to_bytes(1, 'little')

    # If file doesn't exists, return
    if not os.path.isfile(file_path):
        return

    try:
        with open(file_path, 'rb') as f:
            data = f.read()
    except PermissionError:
        return

    data_size = len(data).to_bytes(4, 'little')
    packet = is_identifier + identifier + modify + is_directory + path_size + sent_file_path.encode('utf-8') \
             + data_size + data

    client_socket.sendall(packet)


def send_move_message(client_socket, identifier, base_path, src_path, dest_path, is_directory):
    is_identifier = int(1).to_bytes(1, 'little')
    identifier = identifier.encode('utf-8')
    move = MOVE_COMMAND.to_bytes(1, 'little')
    # Append listening directory name with file path
    sent_src_file_path = os.path.relpath(src_path, base_path)
    src_path_size = len(sent_src_file_path).to_bytes(4, 'little')
    sent_dest_file_path = os.path.relpath(dest_path, base_path)
    dest_path_size = len(sent_dest_file_path).to_bytes(4, 'little')
    is_directory = is_directory.to_bytes(1, 'little')

    packet = is_identifier + identifier + move + is_directory + src_path_size + sent_src_file_path.encode('utf-8') \
             + dest_path_size + sent_dest_file_path.encode('utf-8')

    client_socket.sendall(packet)


class Handler(PatternMatchingEventHandler):
    # Linux OS create temp file with this name when modify file, so we ignore events with this file name
    IGNORE_PATTERN = ".goutputstream"

    def __init__(self, base_path, client_socket, identifier):
        super(Handler, self).__init__(ignore_patterns=[f'*{Handler.IGNORE_PATTERN}*'])
        self.base_path = base_path
        self.client_socket = client_socket
        self.identifier = identifier

    def on_created(self, event):
        send_create_message(self.client_socket, self.identifier, self.base_path, event.src_path,
                            os.path.isdir(event.src_path))

    def on_deleted(self, event):
        send_delete_message(self.client_socket, self.identifier, self.base_path, event.src_path,
                            os.path.isdir(event.src_path))

    def on_modified(self, event):
        # If we got modified event on directory we ignore (Windows OS)
        if os.path.isdir(event.src_path):
            return

        send_modify_message(self.client_socket, self.identifier, self.base_path, event.src_path,
                            os.path.isdir(event.src_path))

    def on_moved(self, event):
        # If src_path is IGNORE_PATTERN it means that the file event.dest_path is just modified, so we send modify event
        # And we ignore the src_path because this is temp file
        if Handler.IGNORE_PATTERN in event.src_path:
            send_modify_message(self.client_socket, self.identifier, self.base_path, event.dest_path,
                                os.path.isdir(event.dest_path))
        else:
            send_move_message(self.client_socket, self.identifier, self.base_path, event.src_path, event.dest_path,
                              os.path.isdir(event.dest_path))


def check_port(n):
    if n.isnumeric() == 0:
        return 0
    n = int(n)
    if 0 <= n <= 65535:
        return 1
    else:
        return 0


def check_ip(n):
    array = n.split(".")
    if len(array) != 4:
        return 0
    else:
        for j in range(0, len(array)):
            if array[j].isnumeric() and 0 <= int(array[j]) <= 255:
                continue
            else:
                return 0
    return 1


if __name__ == "__main__":
    ip = sys.argv[1]
    port_num = sys.argv[2]
    path = os.path.abspath(sys.argv[3])
    time_series = int(sys.argv[4])
    if len(sys.argv) == 6:
        identifier = sys.argv[5]
    else:
        identifier = None

    # If we don't got identifier and the path doesn't exists, we exit the program
    if not identifier and not os.path.isdir(path):
        exit()

    if check_ip(ip) == 0 or check_port(port_num) == 0:
        exit()

    port_num = int(port_num)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port_num))

    try:
        identifier = first_connected_to_server(identifier, s, path)
        start_watchdog(path, s, identifier)
        while True:
            # Set the thread sleep time
            time.sleep(time_series)
            pull_updates_from_server(identifier, s, path)
    except (KeyboardInterrupt, ClientDisconnectedException):
        stop_watchdog()
    wait_observer()
