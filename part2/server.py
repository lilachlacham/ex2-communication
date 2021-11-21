import os
import socket
import string
import sys
import random

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('', 12345))
server.listen(5)


def generate_identifier():
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=128))


def handle_command(identifier, command, client_socket):
    CREATE_COMMAND = 1
    DELETE_COMMAND = 2
    MOVE_COMMAND = 3
    PULL_COMMAND = 4

    if command == CREATE_COMMAND:
        path_size = int(client_socket.recv(4))
        path = identifier + "/" + str(client_socket.recv(path_size))
        file_size = int(client_socket.recv(4))
        file_data = client_socket.recv(file_size)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb+') as f:
            f.write(file_data)

def handle_client(client_socket):
    is_identifier = int(client_socket.recv(1))
    if is_identifier == 0:
        identifier = generate_identifier()
        client_socket.send(identifier)
    else:
        identifier = client_socket.recv(128)
        command = client_socket.recv(1)
        handle_command(identifier, command, client_socket)


while True:
    client_socket, client_address = server.accept()
    while True:
        try:
            handle_client(client_socket)
        except:
            break
    client_socket.close()
