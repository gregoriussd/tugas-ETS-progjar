import random
import string
import os

def create_dummy_ascii_file(file_path, target_size_bytes):
    possible_chars = [chr(i) for i in range(32, 127)] + ['\n']
    bytes_written = 0
    try:
        with open(file_path, 'w', encoding='ascii') as f:
            chunk_size = 4096
            while bytes_written < target_size_bytes:
                remaining_bytes = target_size_bytes - bytes_written
                current_chunk_target_size = min(chunk_size, remaining_bytes)
                random_chars_for_chunk = [random.choice(possible_chars) for _ in range(current_chunk_target_size)]
                random_string_chunk = "".join(random_chars_for_chunk)
                f.write(random_string_chunk)
                bytes_written += len(random_string_chunk)
        
        actual_size_on_disk = os.path.getsize(file_path)
        print(f"File '{file_path}' created. Target: {target_size_bytes} bytes, Actual: {actual_size_on_disk} bytes.")
        if actual_size_on_disk != target_size_bytes:
             print("Note: Actual size differs slightly from target.")
    except IOError as e:
        print(f"Error writing to file '{file_path}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    try:
        file_name_input = input("Enter filename (default: dummy_data.txt): ") or "dummy_data.txt"
        size_mb_input_str = input("Enter file size in MB (default: 1.0): ") or "1.0"
        
        target_size_mb = float(size_mb_input_str)
        if target_size_mb < 0:
            print("Error: File size cannot be negative.")
        else:
            target_size_in_bytes = int(target_size_mb * 1024 * 1024)
            print(f"Creating '{file_name_input}' ({target_size_mb} MB)...")
            create_dummy_ascii_file(file_name_input, target_size_in_bytes)
    except ValueError:
        print("Error: Invalid input for file size. Please enter a number.")
    except Exception as e:
        print(f"An error occurred: {e}")
