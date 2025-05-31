import socket
import json
import os
from concurrent.futures import ThreadPoolExecutor

from file_protocol import FileProtocol
fp = FileProtocol()

STATE_EXPECT_COMMAND = 0
STATE_EXPECT_FILEDATA = 1
STATE_SENDING_FILEDATA = 2

class ProcessTheClient:
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        self.current_state = STATE_EXPECT_COMMAND
        self.active_file_transfer = {}
        self.active_file_send = {}

    def _cleanup_active_transfer_or_send(self, transfer_type="UPLOAD"):
        if transfer_type == "UPLOAD" and self.active_file_transfer:
            file_id = self.active_file_transfer.get('file_id')
            if 'file_handle' in self.active_file_transfer:
                handle = self.active_file_transfer['file_handle']
                if handle and not handle.closed:
                    try: handle.close()
                    except Exception: pass
            if file_id:
                try: fp.proses_string(f"upload_abort {file_id}")
                except Exception: pass 
            self.active_file_transfer = {}
        elif transfer_type == "DOWNLOAD" and self.active_file_send:
            if 'file_handle' in self.active_file_send:
                handle = self.active_file_send['file_handle']
                if handle and not handle.closed:
                    try: handle.close()
                    except Exception: pass
            self.active_file_send = {}
        self.current_state = STATE_EXPECT_COMMAND

    def run(self):
        command_buffer = ""
        try:
            self.connection.settimeout(120.0) 
        except Exception:
            try: self.connection.close()
            except Exception: pass
            return

        while True:
            try:
                if self.current_state == STATE_EXPECT_COMMAND:
                    data_chunk = self.connection.recv(4096)
                    if not data_chunk: break
                    command_buffer += data_chunk.decode('utf-8', errors='ignore')

                    while "\r\n\r\n" in command_buffer:
                        complete_command_str, command_buffer = command_buffer.split("\r\n\r\n", 1)
                        json_response_str = fp.proses_string(complete_command_str)
                        try:
                            response_dict = json.loads(json_response_str)
                        except json.JSONDecodeError:
                            try: self.connection.sendall((json_response_str + "\r\n\r\n").encode())
                            except Exception: pass
                            continue

                        if response_dict.get('status') == 'READY_FOR_DATA' and \
                           'UPLOAD_INITIATE' in complete_command_str.upper() and \
                           response_dict.get('file_id') and \
                           response_dict.get('server_filepath_to_write') and \
                           'expected_size' in response_dict:
                            self.active_file_transfer = {
                                'file_id': response_dict['file_id'],
                                'server_filepath_to_write': response_dict['server_filepath_to_write'],
                                'expected_size': int(response_dict['expected_size']),
                                'current_size': 0, 'file_handle': None
                            }
                            try:
                                os.makedirs(os.path.dirname(self.active_file_transfer['server_filepath_to_write']), exist_ok=True)
                                self.active_file_transfer['file_handle'] = open(self.active_file_transfer['server_filepath_to_write'], 'wb')
                            except IOError:
                                self._cleanup_active_transfer_or_send("UPLOAD")
                                err_resp_str = json.dumps({'status':'ERROR', 'data':'Server failed to prepare file for upload'})
                                try: self.connection.sendall((err_resp_str + "\r\n\r\n").encode())
                                except Exception: pass
                                continue
                            try: self.connection.sendall((json_response_str + "\r\n\r\n").encode())
                            except Exception: break
                            self.current_state = STATE_EXPECT_FILEDATA
                        
                        elif response_dict.get('status') == 'READY_TO_SEND_DATA' and \
                             'GET_STREAM_INITIATE' in complete_command_str.upper() and \
                             response_dict.get('server_filepath_to_read') and \
                             'expected_size' in response_dict:
                            self.active_file_send = {
                                'server_filepath_to_read': response_dict['server_filepath_to_read'],
                                'expected_size': int(response_dict['expected_size']),
                                'bytes_sent': 0, 'file_handle': None
                            }
                            try:
                                self.active_file_send['file_handle'] = open(self.active_file_send['server_filepath_to_read'], 'rb')
                            except IOError:
                                self._cleanup_active_transfer_or_send("DOWNLOAD")
                                err_resp_str = json.dumps({'status':'ERROR', 'data':'Server error: Could not read file'})
                                try: self.connection.sendall((err_resp_str + "\r\n\r\n").encode())
                                except Exception: pass
                                continue
                            try: self.connection.sendall((json_response_str + "\r\n\r\n").encode())
                            except Exception: break
                            self.current_state = STATE_SENDING_FILEDATA
                        
                        else:
                            try: self.connection.sendall((json_response_str + "\r\n\r\n").encode())
                            except Exception: break
                            if response_dict.get('status') == 'OK' and \
                               ('UPLOAD_FINALIZE' in complete_command_str.upper() or \
                                'UPLOAD_ABORT' in complete_command_str.upper()):
                                self.active_file_transfer = {} 

                elif self.current_state == STATE_EXPECT_FILEDATA:
                    if not self.active_file_transfer or not self.active_file_transfer.get('file_handle'):
                        self._cleanup_active_transfer_or_send("UPLOAD")
                        continue
                    fh_upload = self.active_file_transfer['file_handle']
                    bytes_to_receive = min(65536, self.active_file_transfer['expected_size'] - self.active_file_transfer['current_size'])

                    if bytes_to_receive <= 0:
                        file_id = self.active_file_transfer['file_id']
                        if hasattr(fp, 'file_interface') and hasattr(fp.file_interface, 'active_uploads') and file_id in fp.file_interface.active_uploads:
                            try:
                                fp.file_interface.active_uploads[file_id]['current_size'] = self.active_file_transfer['current_size']
                                fp.file_interface.active_uploads[file_id]['status'] = 'CHUNKS_COMPLETE'
                            except Exception: pass
                        if fh_upload and not fh_upload.closed:
                            try: fh_upload.close()
                            except Exception: pass
                        self.active_file_transfer['file_handle'] = None
                        self.current_state = STATE_EXPECT_COMMAND
                        continue
                    file_data_chunk = self.connection.recv(bytes_to_receive)
                    if not file_data_chunk:
                        self._cleanup_active_transfer_or_send("UPLOAD")
                        break 
                    try:
                        fh_upload.write(file_data_chunk)
                        self.active_file_transfer['current_size'] += len(file_data_chunk)
                    except IOError:
                        self._cleanup_active_transfer_or_send("UPLOAD")
                        err_resp_str = json.dumps({'status':'ERROR', 'data':'Server failed to write file data.'})
                        try: self.connection.sendall((err_resp_str + "\r\n\r\n").encode())
                        except Exception: pass
                        break
                
                elif self.current_state == STATE_SENDING_FILEDATA:
                    if not self.active_file_send or not self.active_file_send.get('file_handle'):
                        self._cleanup_active_transfer_or_send("DOWNLOAD")
                        continue
                    fh_download = self.active_file_send['file_handle']
                    if self.active_file_send['bytes_sent'] >= self.active_file_send['expected_size']:
                        self._cleanup_active_transfer_or_send("DOWNLOAD")
                        continue
                    chunk_to_send = fh_download.read(65536)
                    if not chunk_to_send:
                        self._cleanup_active_transfer_or_send("DOWNLOAD")
                        break
                    try:
                        self.connection.sendall(chunk_to_send)
                        self.active_file_send['bytes_sent'] += len(chunk_to_send)
                    except socket.error:
                        self._cleanup_active_transfer_or_send("DOWNLOAD")
                        break 
            
            except UnicodeDecodeError:
                command_buffer = ""
                try:
                    error_response = json.dumps({'status': 'ERROR', 'data': 'Invalid character encoding in command.'}) + "\r\n\r\n"
                    self.connection.sendall(error_response.encode())
                except Exception: pass
                break 
            except ConnectionResetError:
                self._cleanup_active_transfer_or_send("UPLOAD")
                self._cleanup_active_transfer_or_send("DOWNLOAD")
                break
            except socket.timeout:
                self._cleanup_active_transfer_or_send("UPLOAD")
                self._cleanup_active_transfer_or_send("DOWNLOAD")
                break
            except socket.error:
                self._cleanup_active_transfer_or_send("UPLOAD")
                self._cleanup_active_transfer_or_send("DOWNLOAD")
                break
            except Exception:
                self._cleanup_active_transfer_or_send("UPLOAD")
                self._cleanup_active_transfer_or_send("DOWNLOAD")
                try:
                    err_resp_str = json.dumps({'status':'ERROR', 'data':'Unexpected server error processing request.'})
                    self.connection.sendall((err_resp_str + "\r\n\r\n").encode())
                except Exception: pass
                break 

        self._cleanup_active_transfer_or_send("UPLOAD")
        self._cleanup_active_transfer_or_send("DOWNLOAD")
        try:
            self.connection.close()
        except Exception:
            pass

class Server:
    def __init__(self, ipaddress='0.0.0.0', port=8889, max_workers=10):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def run(self):
        print(f"Server starting on {self.ipinfo[0]}:{self.ipinfo[1]} with {self.max_workers} workers")
        
        try:
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(50)
        except Exception as e:
            print(f"ERROR: Could not bind/listen on {self.ipinfo}: {e}") # Minimal error print
            return

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="ClientHandlerPool") as executor:
            try:
                while True:
                    try:
                        connection, client_address = self.my_socket.accept()
                        client_task = ProcessTheClient(connection, client_address)
                        executor.submit(client_task.run)
                    except socket.error:
                        if self.my_socket.fileno() == -1:
                            break 
                    except Exception:
                        pass
            
            except KeyboardInterrupt: 
                print("KeyboardInterrupt received. Shutting down server...")
                pass
            except Exception:
                pass
            finally:
                print("Server loop ended. Shutting down listening socket.")
                try:
                    self.my_socket.close()
                except Exception:
                    pass

def main():
    server_instance = Server(ipaddress='0.0.0.0', port=8889, max_workers=10) 
    server_instance.run()

if __name__ == "__main__":
    main()
