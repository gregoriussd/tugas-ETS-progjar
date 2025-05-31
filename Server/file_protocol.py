import json
import logging
import shlex

from file_interface import FileInterface

class FileProtocol:
    def __init__(self):
        self.file_interface = FileInterface() 

    def proses_string(self, string_datamasuk=''):
        string_datamasuk_cleaned = string_datamasuk.strip()
        try:
            tokens = shlex.split(string_datamasuk_cleaned)
        except ValueError as sve:
            logging.error(f"Shlex parsing error for input '{string_datamasuk_cleaned[:100]}...': {sve}")
            return json.dumps(dict(status='ERROR', data='Invalid command format: Check quoting or special characters.'))
            
        if not tokens:
            return json.dumps(dict(status='ERROR', data='Request kosong'))
        
        c_request = tokens[0].lower() 
        
        params = tokens[1:]

        try:
            if hasattr(self.file_interface, c_request):
                method_to_call = getattr(self.file_interface, c_request)
                hasil = method_to_call(params)
                return json.dumps(hasil)
            else:
                logging.warning(f"Unknown command received: {c_request}")
                return json.dumps(dict(status='ERROR', data=f'Request "{c_request}" tidak dikenali'))
        
        except TypeError as te:
            logging.error(f"TypeError calling {c_request} with params {params}: {te}", exc_info=True)      
            return json.dumps(dict(status='ERROR', data=f'Internal server error: Type error processing request "{c_request}". Check parameters.'))
        except Exception as e:
            logging.error(f"Error processing command '{c_request}' with params {params}: {e}", exc_info=True)
            return json.dumps(dict(status='ERROR', data=f'Error internal server: {str(e)}'))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    fp = FileProtocol()