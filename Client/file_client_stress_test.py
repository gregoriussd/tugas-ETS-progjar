import socket
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import uuid
import csv

server_address = ('127.0.0.1', 8889)

STATUS_OK = "OK"
STATUS_FAIL_CLIENT_PRECONDITION = "FAIL_CLIENT_PRECONDITION"
STATUS_FAIL_CONNECTION_REFUSED = "FAIL_CONNECTION_REFUSED"
STATUS_FAIL_CONNECTION_TIMEOUT_CONNECT = "FAIL_CONNECTION_TIMEOUT_CONNECT"
STATUS_FAIL_SERVER_RESPONSE_ERROR = "FAIL_SERVER_RESPONSE_ERROR"
STATUS_FAIL_SERVER_PROTOCOL = "FAIL_SERVER_PROTOCOL"
STATUS_FAIL_SOCKET_TIMEOUT_OPERATION = "FAIL_SOCKET_TIMEOUT_OPERATION"
STATUS_FAIL_CONNECTION_RESET_OPERATION = "FAIL_CONNECTION_RESET_OPERATION"
STATUS_FAIL_DATA_INTEGRITY = "FAIL_DATA_INTEGRITY"
STATUS_FAIL_UNKNOWN_EXCEPTION = "FAIL_UNKNOWN_EXCEPTION"
STATUS_NO_RESPONSE = "FAIL_NO_RESPONSE"
STATUS_SKIPPED = "SKIPPED"

def is_server_attributed_failure(status_code):
    return status_code in [
        STATUS_FAIL_SERVER_RESPONSE_ERROR,
        STATUS_FAIL_SERVER_PROTOCOL,
        STATUS_FAIL_SOCKET_TIMEOUT_OPERATION,
        STATUS_FAIL_CONNECTION_RESET_OPERATION
    ]

def receive_json_response(sock):
    buffer = ""
    try:
        while True:
            data = sock.recv(4096)
            if not data:
                return STATUS_NO_RESPONSE, {'data': 'No data from server or connection closed'}
            buffer += data.decode('utf-8', errors='ignore')
            if "\r\n\r\n" in buffer:
                json_response_str, _, _ = buffer.partition("\r\n\r\n")
                hasil = json.loads(json_response_str)
                if isinstance(hasil, dict) and hasil.get('status') == 'ERROR':
                    return STATUS_FAIL_SERVER_RESPONSE_ERROR, hasil
                return STATUS_OK, hasil
    except json.JSONDecodeError as je:
        return STATUS_FAIL_SERVER_PROTOCOL, {'data': f'JSON decoding error: {str(je)}'}
    except socket.timeout:
        return STATUS_FAIL_SOCKET_TIMEOUT_OPERATION, {'data': 'Timeout receiving server JSON response'}
    except ConnectionResetError:
        return STATUS_FAIL_CONNECTION_RESET_OPERATION, {'data': 'Connection reset by server receiving JSON response'}
    except Exception as e:
        return STATUS_FAIL_UNKNOWN_EXCEPTION, {'data': f'Generic error in receive_json_response: {str(e)}'}

def send_command_and_get_response(command_str=""):
    global server_address
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(20)
        sock.connect(server_address)
        command_to_send = command_str + "\r\n\r\n"
        sock.sendall(command_to_send.encode())
        status_code, response_data = receive_json_response(sock)
        if status_code == STATUS_OK and isinstance(response_data, dict) and response_data.get('status') == 'ERROR':
            return STATUS_FAIL_SERVER_RESPONSE_ERROR, response_data
        return status_code, response_data
    except ConnectionRefusedError:
        return STATUS_FAIL_CONNECTION_REFUSED, {'data': 'Connection refused'}
    except socket.timeout:
        return STATUS_FAIL_CONNECTION_TIMEOUT_CONNECT, {'data': 'Connection attempt timed out'}
    except Exception as e:
        return STATUS_FAIL_UNKNOWN_EXCEPTION, {'data': str(e)}
    finally:
        if sock:
            sock.close()

def remote_list():
    command_str = "LIST"
    status_code, hasil = send_command_and_get_response(command_str)
    if status_code == STATUS_OK and isinstance(hasil, dict) and hasil.get('status') == 'OK':
        print("Daftar file : ")
        if hasil.get('data'):
            for nmfile in hasil['data']:
                print(f"- {nmfile}")
        else:
            print("Tidak ada file.")
        return True
    else:
        error_message = hasil.get('data', 'Operasi gagal') if isinstance(hasil, dict) else 'Operasi gagal (respons tidak valid)'
        print(f"Gagal menampilkan daftar file: {error_message} (Status: {status_code})")
        return False

def remote_upload_stream(local_filepath, remote_filename=None):
    global server_address
    if not local_filepath or not os.path.exists(local_filepath):
        return STATUS_FAIL_CLIENT_PRECONDITION, {'data': 'Local file invalid or not found'}

    if remote_filename is None:
        remote_filename = os.path.basename(local_filepath)

    file_size = os.path.getsize(local_filepath)
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(300)
        sock.connect(server_address)

        init_command = f"UPLOAD_INITIATE {remote_filename} {file_size}\r\n\r\n"
        sock.sendall(init_command.encode())
        status_code_init, init_response = receive_json_response(sock)

        if status_code_init != STATUS_OK: return status_code_init, init_response
        if not isinstance(init_response, dict) or init_response.get('status') != 'READY_FOR_DATA':
            return STATUS_FAIL_SERVER_PROTOCOL, init_response
        file_id = init_response.get('file_id')
        if not file_id:
            return STATUS_FAIL_SERVER_PROTOCOL, {'data': 'Server READY_FOR_DATA but no file_id'}

        bytes_sent = 0
        chunk_size = 65536
        with open(local_filepath, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk: break
                sock.sendall(chunk)
                bytes_sent += len(chunk)
        
        if bytes_sent != file_size:
            return STATUS_FAIL_DATA_INTEGRITY, {'data': f'Sent {bytes_sent} but expected {file_size}'}

        finalize_command = f"UPLOAD_FINALIZE {file_id}\r\n\r\n"
        sock.sendall(finalize_command.encode())
        status_code_final, final_response = receive_json_response(sock)

        if status_code_final == STATUS_OK and isinstance(final_response, dict) and final_response.get('status') == 'OK':
            return STATUS_OK, final_response
        else:
            err_data = final_response if isinstance(final_response, dict) else {'data': 'Invalid finalize response'}
            return status_code_final if status_code_final != STATUS_OK else STATUS_FAIL_SERVER_RESPONSE_ERROR, err_data
    except ConnectionRefusedError: return STATUS_FAIL_CONNECTION_REFUSED, {'data': 'Connection refused'}
    except socket.timeout: return STATUS_FAIL_SOCKET_TIMEOUT_OPERATION, {'data': 'Timeout during upload operation'}
    except ConnectionResetError: return STATUS_FAIL_CONNECTION_RESET_OPERATION, {'data': 'Connection reset during upload'}
    except socket.error as se:
        return STATUS_FAIL_CONNECTION_RESET_OPERATION, {'data': f'Socket error during data send: {str(se)}'}
    except Exception as e:
        return STATUS_FAIL_UNKNOWN_EXCEPTION, {'data': str(e)}
    finally:
        if sock: sock.close()

def remote_get_stream(remote_filename, local_save_path=None):
    global server_address
    if not remote_filename:
        return STATUS_FAIL_CLIENT_PRECONDITION, {'data': 'Remote filename is empty'}
    if local_save_path is None:
        local_save_path = "downloaded_" + os.path.basename(remote_filename)
    local_dir = os.path.dirname(local_save_path)
    if local_dir and not os.path.exists(local_dir):
        try: os.makedirs(local_dir)
        except OSError as e: return STATUS_FAIL_CLIENT_PRECONDITION, {'data': f'Cannot create local dir {local_dir}: {e}'}

    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(300)
        sock.connect(server_address)

        init_command = f"GET_STREAM_INITIATE {remote_filename}\r\n\r\n"
        sock.sendall(init_command.encode())
        status_code_init, init_response = receive_json_response(sock)

        if status_code_init != STATUS_OK: return status_code_init, init_response
        if not isinstance(init_response, dict) or init_response.get('status') != 'READY_TO_SEND_DATA':
            return STATUS_FAIL_SERVER_PROTOCOL, init_response
        expected_size = init_response.get('expected_size')
        if expected_size is None: return STATUS_FAIL_SERVER_PROTOCOL, {'data': 'Server no expected_size'}
        expected_size = int(expected_size)

        bytes_received = 0
        chunk_size = 65536
        with open(local_save_path, 'wb') as f:
            while bytes_received < expected_size:
                bytes_to_read_now = min(chunk_size, expected_size - bytes_received)
                if bytes_to_read_now <= 0: break
                chunk = sock.recv(bytes_to_read_now)
                if not chunk:
                    return STATUS_FAIL_CONNECTION_RESET_OPERATION, {'data': 'Connection closed by server prematurely during download'}
                f.write(chunk)
                bytes_received += len(chunk)
        
        if bytes_received != expected_size:
            return STATUS_FAIL_DATA_INTEGRITY, {'data': f'Received {bytes_received} but expected {expected_size}'}
        return STATUS_OK, {'data': f'File {remote_filename} downloaded to {local_save_path}'}
    except ConnectionRefusedError: return STATUS_FAIL_CONNECTION_REFUSED, {'data': 'Connection refused'}
    except socket.timeout: return STATUS_FAIL_SOCKET_TIMEOUT_OPERATION, {'data': 'Timeout during download operation'}
    except ConnectionResetError: return STATUS_FAIL_CONNECTION_RESET_OPERATION, {'data': 'Connection reset during download'}
    except socket.error as se:
        return STATUS_FAIL_CONNECTION_RESET_OPERATION, {'data': f'Socket error during data recv: {str(se)}'}
    except Exception as e:
        return STATUS_FAIL_UNKNOWN_EXCEPTION, {'data': str(e)}
    finally:
        if sock: sock.close()

def remote_delete(filename=""):
    if not filename:
        return STATUS_FAIL_CLIENT_PRECONDITION, {'data': 'Filename for delete is empty'}
    command_str = f"DELETE {filename}"
    status_code, hasil = send_command_and_get_response(command_str)
    if status_code == STATUS_OK and isinstance(hasil, dict) and hasil.get('status') == 'OK':
        return STATUS_OK, hasil
    else:
        err_data = hasil if isinstance(hasil, dict) else {'data': 'Invalid delete response'}
        return status_code if status_code != STATUS_OK else STATUS_FAIL_SERVER_RESPONSE_ERROR, err_data

def perform_stress_test_cycle(worker_id, local_file_info, test_run_id_str):
    local_filepath = local_file_info['path']
    original_filename = os.path.basename(local_filepath)
    unique_suffix = f"_w{worker_id}_run{test_run_id_str}_{str(uuid.uuid4())[:4]}"
    remote_upload_name = f"stress_{original_filename.split('.')[0]}{unique_suffix}.{original_filename.split('.')[-1] if '.' in original_filename else 'dat'}"
    download_dir = os.path.join("client_downloads", f"run_{test_run_id_str}", f"worker_{worker_id}")
    os.makedirs(download_dir, exist_ok=True)
    local_download_path = os.path.join(download_dir, f"downloaded_{remote_upload_name}")

    results = {
        'worker_id': worker_id, 'file_id': local_file_info['id'], 'file_size_bytes': local_file_info['size_bytes'],
        'upload_detailed_status': STATUS_FAIL_UNKNOWN_EXCEPTION, 'upload_time_s': None, 'upload_error_info': None,
        'download_detailed_status': STATUS_FAIL_UNKNOWN_EXCEPTION, 'download_time_s': None, 'download_error_info': None,
        'delete_detailed_status': STATUS_SKIPPED, 'delete_error_info': "Delete operation skipped for inspection"
    }

    upload_start_time = time.monotonic()
    up_status, up_data = remote_upload_stream(local_filepath, remote_upload_name)
    results['upload_time_s'] = time.monotonic() - upload_start_time
    results['upload_detailed_status'] = up_status
    results['upload_error_info'] = up_data.get('data') if up_status != STATUS_OK and isinstance(up_data, dict) else None
    if up_status != STATUS_OK: return results

    download_start_time = time.monotonic()
    dl_status, dl_data = remote_get_stream(remote_upload_name, local_download_path)
    results['download_time_s'] = time.monotonic() - download_start_time
    results['download_detailed_status'] = dl_status
    results['download_error_info'] = dl_data.get('data') if dl_status != STATUS_OK and isinstance(dl_data, dict) else None
    if dl_status == STATUS_OK:
        if not (os.path.exists(local_download_path) and os.path.getsize(local_download_path) == local_file_info['size_bytes']):
            size_on_disk = os.path.getsize(local_download_path) if os.path.exists(local_download_path) else 'N/A'
            results['download_detailed_status'] = STATUS_FAIL_DATA_INTEGRITY
            results['download_error_info'] = f"Size mismatch: expected {local_file_info['size_bytes']}, got {size_on_disk}"
    return results

if __name__ == '__main__':
    server_address = ('172.16.16.101', 8889)

    STRESS_TEST_FILES_CONFIG = [
        {'id': '10MB', 'path': 'dummy_file_10MB.txt', 'size_bytes': 10 * 1024 * 1024},
        {'id': '50MB', 'path': 'dummy_file_50MB.txt', 'size_bytes': 50 * 1024 * 1024},
        {'id': '100MB', 'path': 'dummy_file_100MB.txt', 'size_bytes': 100 * 1024 * 1024},
    ]
    valid_test_files = []
    for f_info in STRESS_TEST_FILES_CONFIG:
        if os.path.exists(f_info['path']) and os.path.getsize(f_info['path']) == f_info['size_bytes']:
            valid_test_files.append(f_info)
    if not valid_test_files:
        print("Tidak ada file dummy yang valid. Lewati stress test.")
    else:
        CONCURRENT_CLIENT_WORKERS_LIST = [1, 5, 10]
        SERVER_WORKER_POOL_SIZE_CONFIG = 10

        csv_filename = f"stress_test_results_{time.strftime('%Y%m%d_%H%M%S')}.csv"
        csv_fieldnames = [
            'Nomor', 'Operasi', 'Volume', 'Jumlah client worker pool',
            'Jumlah server worker pool', 'Waktu total per client', 'Throughput per client',
            'Jumlah client worker berhasil', 'Jumlah client worker gagal',
            'Jumlah server worker berhasil', 'Jumlah server worker gagal'
        ]
        with open(csv_filename, 'w', newline='') as csvfile_main:
            writer_main = csv.DictWriter(csvfile_main, fieldnames=csv_fieldnames)
            writer_main.writeheader()

        test_run_id_counter = 0
        for num_client_workers in CONCURRENT_CLIENT_WORKERS_LIST:
            for file_info_to_test in valid_test_files:
                test_run_id_counter += 1
                test_run_id_str = f"{test_run_id_counter:03d}"

                print(f"Memulai stress test run #{test_run_id_str} - File: {file_info_to_test['id']}, Klien: {num_client_workers}, Server Workers (Config): {SERVER_WORKER_POOL_SIZE_CONFIG}")
                
                all_run_results = []
                start_time_scenario = time.monotonic()

                with ThreadPoolExecutor(max_workers=num_client_workers, thread_name_prefix=f"StressClient_F{file_info_to_test['id']}") as executor:
                    futures = [executor.submit(perform_stress_test_cycle, i, file_info_to_test, test_run_id_str) for i in range(num_client_workers)]
                    for future in futures:
                        try: all_run_results.append(future.result())
                        except Exception as e_future:
                            all_run_results.append({'worker_id': 'N/A_FutureError', 'file_id': file_info_to_test['id'],
                                                    'upload_detailed_status': STATUS_FAIL_UNKNOWN_EXCEPTION,
                                                    'download_detailed_status': STATUS_FAIL_UNKNOWN_EXCEPTION,
                                                    'delete_detailed_status': STATUS_FAIL_UNKNOWN_EXCEPTION,
                                                    'error_future': str(e_future)})
                
                total_time_scenario = time.monotonic() - start_time_scenario

                for op_name, status_key, time_key in [("Upload", "upload_detailed_status", "upload_time_s"),
                                                      ("Download", "download_detailed_status", "download_time_s")]:
                    op_ok_count = 0; op_fail_server_count = 0; op_fail_other_count = 0
                    op_times = []
                    for r_item in all_run_results:
                        op_status = r_item.get(status_key)
                        if op_status == STATUS_OK:
                            op_ok_count += 1
                            if r_item.get(time_key) is not None: op_times.append(r_item[time_key])
                        elif is_server_attributed_failure(op_status):
                            op_fail_server_count += 1
                        else:
                            op_fail_other_count += 1
                    
                    op_fail_total_count = op_fail_server_count + op_fail_other_count
                    avg_op_time = sum(op_times) / len(op_times) if op_times else 0
                    avg_op_MBps_per_client = (file_info_to_test['size_bytes'] / (1024*1024)) / avg_op_time if avg_op_time > 0 else 0
                    server_worker_gagal = op_fail_server_count
                    server_worker_berhasil = max(0, SERVER_WORKER_POOL_SIZE_CONFIG - server_worker_gagal)

                    with open(csv_filename, 'a', newline='') as csvfile_append:
                        writer_append = csv.DictWriter(csvfile_append, fieldnames=csv_fieldnames)
                        writer_append.writerow({
                            'Nomor': test_run_id_str, 'Operasi': op_name, 'Volume': file_info_to_test['id'],
                            'Jumlah client worker pool': num_client_workers,
                            'Jumlah server worker pool': SERVER_WORKER_POOL_SIZE_CONFIG,
                            'Waktu total per client': f"{avg_op_time:.3f}" if op_times else "N/A",
                            'Throughput per client': f"{avg_op_MBps_per_client:.3f}" if avg_op_MBps_per_client > 0 else "N/A",
                            'Jumlah client worker berhasil': op_ok_count,
                            'Jumlah client worker gagal': op_fail_total_count,
                            'Jumlah server worker berhasil': server_worker_berhasil,
                            'Jumlah server worker gagal': server_worker_gagal
                        })
                    print(f"  {op_name}: {op_ok_count} OK, {op_fail_server_count} Gagal (Server), {op_fail_other_count} Gagal (Lainnya).")
                    if op_times: print(f"    Rata-rata Waktu {op_name}: {avg_op_time:.3f}s, Throughput: {avg_op_MBps_per_client:.3f} MB/s")
                print(f"Total Waktu Skenario Keseluruhan: {total_time_scenario:.2f} detik")
                print("--------------------------------------------------")
        print(f"\n--- Semua Tes Selesai. Hasil disimpan di {csv_filename} ---")