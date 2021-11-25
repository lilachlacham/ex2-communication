import os
import socket
import string
import sys
import random

CREATE_COMMAND = 1
DELETE_COMMAND = 2
MODIFY_COMMAND = 3
PULL_COMMAND = 4

port = int(sys.argv[1])
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('', port))
server.settimeout(0.2)
server.listen()

file_changes_dict = {}
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


def send_file_to_client(identifier, path, client_socket):
    send_path = os.path.relpath(path, identifier)
    packet = CREATE_COMMAND.to_bytes(1, 'little')
    packet += len(send_path).to_bytes(4, 'little')
    packet += send_path.encode('utf-8')
    with open(path, 'rb') as f:
        file_data = f.read()

    packet += len(file_data).to_bytes(4, 'little')
    packet += file_data

    client_socket.send(packet)


def send_empty_file_to_client(client_socket):
    packet = int(0).to_bytes(1, 'little')
    client_socket.send(packet)


def send_all_directory_to_client(path, identifier, client_socket):
    for root, subdirs, files in os.walk(path):
        for file in files:
            send_file_to_client(identifier, os.path.join(root, file), client_socket)

    send_empty_file_to_client(client_socket)


def handle_command(identifier, command, client_socket, client_address):
    packet = b''
    if command == CREATE_COMMAND:
        packet = create_command(client_socket, command, identifier)
    elif command == PULL_COMMAND:
        send_all_directory_to_client(identifier, identifier, client_socket)
    if command != PULL_COMMAND:
        add_packet_to_update_dict(packet, identifier, client_address)


def create_command(client_socket, command, identifier):
    is_directory = int.from_bytes(client_socket.recv(1), 'little')
    path_size = int.from_bytes(client_socket.recv(4), 'little')
    path = os.path.join(identifier, client_socket.recv(path_size).decode('utf-8'))

    packet = command.to_bytes(1, 'little')
    packet = is_directory.to_bytes(1, 'little')
    packet += path_size.to_bytes(4, 'little')
    packet += os.path.relpath(path, identifier).encode('utf-8')

    if is_directory:
        os.makedirs(path, exist_ok=True)
        return packet

    file_size = int.from_bytes(client_socket.recv(4), 'little')
    file_data = client_socket.recv(file_size)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb+') as f:
        f.write(file_data)
    packet += file_size.to_bytes(4, 'little')
    packet += file_data
    return packet


def add_client_to_file_dict(identifier, client_address):
    if identifier not in file_changes_dict:
        file_changes_dict[identifier] = {client_address: []}
    else:
        identifier_dict = file_changes_dict[identifier]
        if client_address not in identifier_dict:
            identifier_dict[client_address] = []


def update_client(client_socket, identifier, client_address):
    packets_to_send = file_changes_dict[identifier][client_address]
    for packet_to_send in packets_to_send:
        client_socket.send(packet_to_send)

    packets_to_send.clear()


def handle_client(client_socket, client_address):
    is_identifier = client_socket.recv(1)
    if not is_identifier:
        raise ClientDisconnectedException()
    is_identifier = int.from_bytes(is_identifier, 'little')
    if is_identifier == 0:
        identifier = generate_identifier()
        client_socket.send(identifier.encode('utf-8'))
    else:
        identifier = client_socket.recv(128).decode('utf-8')
        command = int.from_bytes(client_socket.recv(1), 'little')
        handle_command(identifier, command, client_socket, client_address)

    add_client_to_file_dict(identifier, client_address)
    update_client(client_socket, identifier, client_address)


def handle_all_clients():
    removed_sockets = []
    global client_sockets
    for client_socket, client_address in client_sockets:
        while True:
            try:
                handle_client(client_socket, client_address)
            except socket.timeout:
                break
            except ClientDisconnectedException:
                removed_sockets.append((client_socket, client_address))
                break

    client_sockets = list(set(client_sockets) - set(removed_sockets))


while True:
    try:
        client_socket, client_address = server.accept()
        client_socket.settimeout(2)
        client_sockets.append((client_socket, client_address))
    except socket.timeout:
        handle_all_clients()
        continue

    while True:
        try:
            handle_client(client_socket, client_address)
        except socket.timeout:
            break
        except ClientDisconnectedException:
            client_sockets.remove((client_socket, client_address))
            break
