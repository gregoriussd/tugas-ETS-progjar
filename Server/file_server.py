from socket import *
import socket
import threading
import logging
import time
import sys
import json

from file_protocol import FileProtocol
fp = FileProtocol()


class ProcessTheClient(threading.Thread):
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        threading.Thread.__init__(self)

    def run(self):
        command_buffer = ""
        while True:
            try:
                data_chunk = self.connection.recv(4096) 
                if not data_chunk:
                    break

                command_buffer += data_chunk.decode()
                
                while "\r\n\r\n" in command_buffer:
                    complete_command, command_buffer = command_buffer.split("\r\n\r\n", 1)
                    
                    hasil = fp.proses_string(complete_command) 
                    
                    response_to_send = hasil + "\r\n\r\n"
                    self.connection.sendall(response_to_send.encode())

            except ConnectionResetError:
                logging.warning(f"Connection reset by client {self.address}.")
                break
            
            except socket.timeout:
                logging.warning(f"Socket timeout for client {self.address}.")
                break
            
            except Exception as e:
                logging.error(f"Error processing client {self.address}: {e}", exc_info=True)
                try:
                    error_response = json.dumps({'status': 'ERROR', 'data': f'Server processing error: {str(e)}'}) + "\r\n\r\n"
                    self.connection.sendall(error_response.encode())
                except Exception as send_err:
                    logging.error(f"Failed to send error response to {self.address}: {send_err}")
                break

        logging.warning(f"Closing connection from {self.address}.")
        self.connection.close()


class Server(threading.Thread):
    def __init__(self,ipaddress='0.0.0.0',port=8889):
        self.ipinfo=(ipaddress,port)
        self.the_clients = []
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        threading.Thread.__init__(self)

    def run(self):
        logging.warning(f"Server berjalan di ip address {self.ipinfo}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        while True:
            try:
                self.connection, self.client_address = self.my_socket.accept()
                logging.warning(f"Connection from {self.client_address}")

                clt = ProcessTheClient(self.connection, self.client_address)
                clt.start()
                self.the_clients.append(clt)
            except Exception as e:
                logging.error(f"Error accepting new connection: {e}", exc_info=True)

def main():
    svr = Server()
    svr.start()


if __name__ == "__main__":
    main()