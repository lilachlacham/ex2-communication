import os
import socket
import string
import sys
import random

CREATE_COMMAND = 1
DELETE_COMMAND = 2
MODIFY_COMMAND = 3
MOVE_COMMAND = 4
PULL_COMMAND = 5
UPDATES_COMMAND = 6

# Dictionary that save changes of each client by his ID
file_changes_dict = {}
# Array of client sockets and address
client_sockets = []


class ClientDisconnectedException(BaseException):
    def __init__(self):
        super().__init__(self, "Client Disconnected")


def generate_identifier():
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=128))


def add_packet_to_update_dict(packet, identifier, client_address):
    identifier_dict = file_changes_dict[identifier]
    for address in identifier_dict:
        if client_address == address:
            continue
        identifier_dict[address].append(packet)


def recv(client_socket, recv_size):
    recv_data = b''
    while len(recv_data) < recv_size:
        recv_data += client_socket.recv(recv_size - len(recv_data))

    return recv_data


def send_file_to_client(identifier, path, client_socket):
    send_path = os.path.relpath(path, identifier)
    packet = CREATE_COMMAND.to_bytes(1, 'little')
    is_directory = os.path.isdir(path).to_bytes(1, 'little')
    packet += is_directory
    packet += len(send_path).to_bytes(4, 'little')
    packet += send_path.encode('utf-8')
    if os.path.isdir(path):
        client_socket.sendall(packet)
        return

    with open(path, 'rb') as f:
        file_data = f.read()

    packet += len(file_data).to_bytes(4, 'little')
    packet += file_data

    client_socket.sendall(packet)


# Indicates that we reach all of files
def send_empty_file_to_client(client_socket):
    packet = int(0).to_bytes(1, 'little')
    client_socket.sendall(packet)


def send_all_directory_to_client(path, identifier, client_socket):
    for root, subdirs, files in os.walk(path):
        for file in files:
            send_file_to_client(identifier, os.path.join(root, file), client_socket)
        for subdir in subdirs:
            if not os.listdir(os.path.join(root, subdir)):
                send_file_to_client(identifier, os.path.join(root, subdir), client_socket)

    # Send empty message to indicates we sent all files
    send_empty_file_to_client(client_socket)


def create_command(client_socket, identifier):
    is_directory = int.from_bytes(client_socket.recv(1), 'little')
    path_size = int.from_bytes(client_socket.recv(4), 'little')
    path = os.path.join(identifier, client_socket.recv(path_size).decode('utf-8'))
    path = path.replace("/", os.sep)
    path = path.replace('\\', os.sep)

    packet = CREATE_COMMAND.to_bytes(1, 'little')
    packet += is_directory.to_bytes(1, 'little')
    packet += path_size.to_bytes(4, 'little')
    packet += os.path.relpath(path, identifier).encode('utf-8')

    if is_directory:
        # If already created so return with empty update packet
        if os.path.isdir(path):
            return b''
        os.makedirs(path, exist_ok=True)
        return packet

    file_size = int.from_bytes(client_socket.recv(4), 'little')
    file_data = recv(client_socket, file_size)

    # If already exists the same file we return with empty update packet
    if os.path.isfile(path):
        with open(path, 'rb') as f:
            current_data = f.read()
            if current_data == file_data:
                return b''

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, 'wb+') as f:
        f.write(file_data)
    packet += file_size.to_bytes(4, 'little')
    packet += file_data
    return packet


def delete_recursive(path):
    for root, subdirs, files in os.walk(path, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for subdir in subdirs:
            os.rmdir(os.path.join(root, subdir))
    if os.path.isdir(path):
        os.rmdir(path)


def delete_command(client_socket, identifier):
    is_directory = int.from_bytes(client_socket.recv(1), 'little')
    path_size = int.from_bytes(client_socket.recv(4), 'little')
    sent_path = client_socket.recv(path_size).decode('utf-8')
    path = os.path.join(identifier, sent_path)
    path = path.replace("/", os.sep)
    path = path.replace('\\', os.sep)

    # If file/directory does not exists, return with empty update packet
    if not os.path.isfile(path) and not os.path.isdir(path):
        return b''

    if not os.path.isdir(path):
        os.remove(path)
    else:
        delete_recursive(path)

    return DELETE_COMMAND.to_bytes(1, 'little') + is_directory.to_bytes(1, 'little') + path_size.to_bytes(4, 'little') \
           + sent_path.encode('utf-8')


def modify_command(client_socket, identifier):
    is_directory = int.from_bytes(client_socket.recv(1), 'little')
    path_size = int.from_bytes(client_socket.recv(4), 'little')
    sent_path = client_socket.recv(path_size).decode('utf-8')
    path = os.path.join(identifier, sent_path)
    path = path.replace("/", os.sep)
    path = path.replace('\\', os.sep)
    file_size = int.from_bytes(client_socket.recv(4), 'little')
    file_data = recv(client_socket, file_size)

    os.makedirs(os.path.dirname(path), exist_ok=True)

    # If already exists the same file, we return with empty update packet
    if os.path.isfile(path):
        with open(path, 'rb') as f:
            current_data = f.read()
            if current_data == file_data:
                return b''

    with open(path, 'wb+') as f:
        f.write(file_data)

    return MODIFY_COMMAND.to_bytes(1, 'little') + is_directory.to_bytes(1, 'little') + path_size.to_bytes(4, 'little') \
           + sent_path.encode('utf-8') + file_size.to_bytes(4, 'little') + file_data


def move_command(client_socket, identifier):
    is_directory = int.from_bytes(client_socket.recv(1), 'little')
    src_path_size = int.from_bytes(client_socket.recv(4), 'little')
    sent_src_path = client_socket.recv(src_path_size).decode('utf-8')
    src_path = os.path.join(identifier, sent_src_path)
    src_path = src_path.replace("/", os.sep)
    src_path = src_path.replace('\\', os.sep)
    dst_path_size = int.from_bytes(client_socket.recv(4), 'little')
    sent_dst_path = client_socket.recv(dst_path_size).decode('utf-8')
    dst_path = os.path.join(identifier, sent_dst_path)
    dst_path = dst_path.replace("/", os.sep)
    dst_path = dst_path.replace('\\', os.sep)

    # If file/directory does not exists, return with empty update packet
    if not os.path.isfile(src_path) and not os.path.isdir(src_path):
        return b''

    # Remove destination file if exists
    if not is_directory and os.path.isfile(dst_path):
        os.remove(dst_path)

    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    # If path is dir and the source dir is empty dir, we delete it, otherwise rename the file
    if is_directory and os.path.isdir(dst_path):
        if not os.listdir(src_path):
            os.rmdir(src_path)
    else:
        os.rename(src_path, dst_path)

    return MOVE_COMMAND.to_bytes(1, 'little') + is_directory.to_bytes(1, 'little') + src_path_size.to_bytes(4, 'little') \
           + sent_src_path.encode('utf-8') + dst_path_size.to_bytes(4, 'little') + sent_dst_path.encode('utf-8')


def handle_command(identifier, command, client_socket, client_address):
    packet = b''
    if command == CREATE_COMMAND:
        packet = create_command(client_socket, identifier)
    elif command == DELETE_COMMAND:
        packet = delete_command(client_socket, identifier)
    elif command == MODIFY_COMMAND:
        packet = modify_command(client_socket, identifier)
    elif command == MOVE_COMMAND:
        packet = move_command(client_socket, identifier)
    elif command == PULL_COMMAND:
        send_all_directory_to_client(identifier, identifier, client_socket)
    elif command == UPDATES_COMMAND:
        update_client(client_socket, identifier, client_address)

    if packet:
        add_packet_to_update_dict(packet, identifier, client_address)


def add_client_to_file_dict(identifier, client_address):
    if identifier not in file_changes_dict:
        file_changes_dict[identifier] = {client_address: []}
    else:
        identifier_dict = file_changes_dict[identifier]
        if client_address not in identifier_dict:
            identifier_dict[client_address] = []


# Send all update packets to client
def update_client(client_socket, identifier, client_address):
    packets_to_send = file_changes_dict[identifier][client_address]
    client_socket.sendall(len(packets_to_send).to_bytes(4, 'little'))

    for packet_to_send in packets_to_send:
        client_socket.sendall(packet_to_send)

    packets_to_send.clear()


def handle_client(client_socket, client_address):
    is_identifier = client_socket.recv(1)
    # If is_identifier is empty array so the client disconnected
    if not is_identifier:
        raise ClientDisconnectedException()

    is_identifier = int.from_bytes(is_identifier, 'little')
    # If not received identifier we generate one and send it to client, otherwise handle client command
    if is_identifier == 0:
        identifier = generate_identifier()
        print(identifier)
        os.makedirs(identifier, exist_ok=True)
        client_socket.sendall(identifier.encode('utf-8'))
    else:
        identifier = client_socket.recv(128).decode('utf-8')
        command = int.from_bytes(client_socket.recv(1), 'little')
        # If client identify with invalid identifier we send him error code (-1)
        if not os.path.isdir(identifier):
            client_socket.sendall(int(-1).to_bytes(1, 'little', signed=True))
            client_socket.close()
            raise ClientDisconnectedException()
        handle_command(identifier, command, client_socket, client_address)

    # Add client to dict of update changes
    add_client_to_file_dict(identifier, client_address)


def handle_all_clients():
    removed_sockets = []
    global client_sockets
    for client_socket, client_address in client_sockets:
        while True:
            try:
                handle_client(client_socket, client_address)
            except socket.timeout:
                break
            except (ClientDisconnectedException, ConnectionResetError):
                removed_sockets.append((client_socket, client_address))
                break

    # Remove all clients that disconnected
    client_sockets = list(set(client_sockets) - set(removed_sockets))
    for remove_socket in removed_sockets:
        remove_client_from_dict(remove_socket[1])


def remove_client_from_dict(client_address):
    for identifier in file_changes_dict:
        if client_address in file_changes_dict[identifier]:
            del file_changes_dict[identifier][client_address]
            break


port = int(sys.argv[1])
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('', port))
server.settimeout(0.2)
server.listen()

try:
    while True:
        try:
            # Wait for connection of new client for 0.2 seconds
            client_socket, client_address = server.accept()
            client_socket.settimeout(2)
            client_sockets.append((client_socket, client_address))
        except socket.timeout:
            # If not accept new client, we iterate all existing clients and handle their commands
            handle_all_clients()
            continue

        while True:
            try:
                # If new client accepted, handle him until non response timeout of 2 seconds
                handle_client(client_socket, client_address)
            except socket.timeout:
                break
            except (ClientDisconnectedException, ConnectionResetError):
                # If client disconnected we remove him for our lists
                client_sockets.remove((client_socket, client_address))
                remove_client_from_dict(client_address)
                break
except KeyboardInterrupt:
    pass
