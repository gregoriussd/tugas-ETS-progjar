import os
import json
import base64
from glob import glob
import uuid

UPLOAD_DIR = 'files/'

class FileInterface:
    def __init__(self):
        self.active_uploads = {}
        if not os.path.exists(UPLOAD_DIR):
            os.makedirs(UPLOAD_DIR)

    def _get_full_path(self, filename):
        return os.path.join(UPLOAD_DIR, os.path.basename(filename))

    def list(self, params=[]):
        try:
            filelist = [f for f in os.listdir(UPLOAD_DIR) if os.path.isfile(os.path.join(UPLOAD_DIR, f))]
            return dict(status='OK', data=filelist)
        except Exception as e:
            return dict(status='ERROR', data=str(e))
            
    def delete(self, params=[]):
        if not params or not params[0]:
            return dict(status='ERROR', data='Filename for delete cannot be empty')
        try:
            filename = params[0]
            filepath = self._get_full_path(filename)
            if not os.path.exists(filepath):
                return dict(status='ERROR', data=f"File {filename} not found for deletion")
            os.remove(filepath)
            return dict(status='OK', data=f"File {filename} deleted successfully")
        except Exception as e:
            return dict(status='ERROR', data=f"Error during delete: {str(e)}")

    def upload_initiate(self, params=[]):
        if len(params) < 2:
            return dict(status='ERROR', data='UPLOAD_INITIATE requires client_filename and expected_size')
        
        client_filename = params[0]
        try:
            expected_size = int(params[1])
            if expected_size < 0: raise ValueError("Size invalid")
        except ValueError:
            return dict(status='ERROR', data='Invalid expected_size for UPLOAD_INITIATE')

        if not client_filename:
            return dict(status='ERROR', data='Client filename for UPLOAD_INITIATE cannot be empty')

        file_id = str(uuid.uuid4())
        server_filename = os.path.basename(client_filename) 
        server_filepath_part = self._get_full_path(f"{file_id}_{server_filename}.part") 

        self.active_uploads[file_id] = {
            'original_filename': client_filename,
            'server_filepath_part': server_filepath_part, 
            'final_filepath': self._get_full_path(server_filename), 
            'expected_size': expected_size,
            'current_size': 0,
            'status': 'INITIATED'
        }
        return dict(
            status='READY_FOR_DATA', 
            file_id=file_id,
            server_filepath_to_write=server_filepath_part, 
            expected_size=expected_size
        )

    def upload_finalize(self, params=[]):
        if not params or not params[0]:
            return dict(status='ERROR', data='UPLOAD_FINALIZE requires file_id')
        file_id = params[0]

        upload_info = self.active_uploads.get(file_id)
        if not upload_info:
            return dict(status='ERROR', data=f'Invalid file_id for finalize: {file_id}')

        part_file = upload_info['server_filepath_part']
        final_file = upload_info['final_filepath']

        if upload_info.get('status') != 'CHUNKS_COMPLETE' and upload_info.get('current_size') != upload_info.get('expected_size'):
             pass

        if not os.path.exists(part_file):
            self.upload_abort([file_id]) 
            return dict(status='ERROR', data='Upload data not found for finalization.')

        actual_size = os.path.getsize(part_file)
        if actual_size != upload_info['expected_size']:
            self.upload_abort([file_id]) 
            return dict(status='ERROR', data=f'File size mismatch. Expected {upload_info["expected_size"]}, received {actual_size}.')

        try:
            if os.path.exists(final_file):
                 os.remove(final_file)
            os.rename(part_file, final_file)
            del self.active_uploads[file_id]
            return dict(status='OK', data=f"File {upload_info['original_filename']} uploaded successfully.")
        except Exception as e:
            self.upload_abort([file_id]) 
            return dict(status='ERROR', data=f'Server error during file finalization: {str(e)}')

    def upload_abort(self, params=[]):
        if not params or not params[0]:
            return dict(status='ERROR', data='UPLOAD_ABORT requires file_id')
        file_id = params[0]
        upload_info = self.active_uploads.pop(file_id, None) 
        if upload_info:
            part_file = upload_info['server_filepath_part']
            if os.path.exists(part_file):
                try:
                    os.remove(part_file)
                except OSError:
                    pass # Tidak ada logging error
            return dict(status='OK', data=f"Upload {file_id} aborted.")
        else:
            return dict(status='OK', data=f"Upload {file_id} not found or already aborted.")

    def get_stream_initiate(self, params=[]):
        if not params or not params[0]:
            return dict(status='ERROR', data='GET_STREAM_INITIATE requires a filename')
        
        filename_to_send = params[0]
        if not filename_to_send:
            return dict(status='ERROR', data='Filename for GET_STREAM_INITIATE cannot be empty')

        filepath_to_read = self._get_full_path(filename_to_send)

        if not os.path.exists(filepath_to_read) or not os.path.isfile(filepath_to_read):
            return dict(status='ERROR', data=f"File '{filename_to_send}' not found on server.")
        try:
            expected_size = os.path.getsize(filepath_to_read)
            file_id_for_log = str(uuid.uuid4())
            return dict(
                status='READY_TO_SEND_DATA',
                file_id=file_id_for_log,
                server_filepath_to_read=filepath_to_read,
                expected_size=expected_size
            )
        except Exception as e:
            return dict(status='ERROR', data=f"Server error preparing file '{filename_to_send}': {str(e)}")