"""
Downloads Wikipedia pages up-to a certain folder size limit. Used for stress testing upload/download speeds.
"""

import os
import random
import bz2
import urllib.request
import time

TARGET_TOTAL_SIZE_GB = 3.0
MAX_FILE_SIZE_MB = 1.1  # Keeping it just under 2MB
BASE_DIR = "/Volumes/HomeXx/compuir/texts"
SUBFOLDER_COUNT = 30 # How many random folders to scatter files into
WIKI_URL = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"

# Constants
BYTES_PER_MB = 1024 * 1024
BYTES_PER_FILE = int(MAX_FILE_SIZE_MB * BYTES_PER_MB)
TARGET_TOTAL_BYTES = int(TARGET_TOTAL_SIZE_GB * 1024 * 1024 * 1024)

def setup_directories():
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
        print(f"Created base directory: {BASE_DIR}")
    
    subfolders = []
    for i in range(SUBFOLDER_COUNT):
        folder_name = f"batch_{i:03d}"
        path = os.path.join(BASE_DIR, folder_name)
        os.makedirs(path, exist_ok=True)
        subfolders.append(path)
    return subfolders

def stream_and_scatter(subfolders):
    print(f"Starting stream from: {WIKI_URL}")
    print(f"Target: {TARGET_TOTAL_SIZE_GB} GB | File limit: {MAX_FILE_SIZE_MB} MB")
    print("This may take time depending on your internet connection...")

    total_bytes_written = 0
    files_created = 0
    
    # Connect to the URL
    with urllib.request.urlopen(WIKI_URL) as response:
        decompressor = bz2.BZ2Decompressor()
        buffer = b""
        
        while total_bytes_written < TARGET_TOTAL_BYTES:
            # Read a chunk from the network
            chunk = response.read(1024 * 64) # Read 64KB chunks
            if not chunk:
                break
            
            # Decompress the chunk and add to buffer
            try:
                decompressed_data = decompressor.decompress(chunk)
                buffer += decompressed_data
                # buffer += chunk

            except EOFError:
                break # End of stream

            # While we have enough data in the buffer to make a file
            while len(buffer) >= BYTES_PER_FILE:
                # Slice off the chunk for the file
                file_content = buffer[:BYTES_PER_FILE]
                buffer = buffer[BYTES_PER_FILE:] # Keep the remainder
                
                # Pick a random folder
                target_folder = random.choice(subfolders)
                filename = f"wiki_segment_{files_created:05d}.txt"
                filepath = os.path.join(target_folder, filename)
                
                # Write the file
                with open(filepath, "wb") as f:
                    f.write(file_content)
                
                total_bytes_written += len(file_content)
                files_created += 1
                
                # Progress log every 50 files
                if files_created % 50 == 0:
                    gb_written = total_bytes_written / (1024**3)
                    print(f"Progress: {files_created} files created ({gb_written:.2f} GB / {TARGET_TOTAL_SIZE_GB} GB)")
                
                if total_bytes_written >= TARGET_TOTAL_BYTES:
                    break

    print("--- Finished ---")
    print(f"Total Files: {files_created}")
    print(f"Total Size: {total_bytes_written / (1024**3):.2f} GB")
    print(f"Location: {BASE_DIR}")

if __name__ == "__main__":
    try:
        folders = setup_directories()
        stream_and_scatter(folders)
    except KeyboardInterrupt:
        print("\nProcess stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")