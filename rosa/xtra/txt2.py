"""
Downloads Wikipedia pages and evenly distributes them across all directories.
Ensures balanced file distribution for consistent testing scenarios.
"""

import os
import random
import bz2
import urllib.request
import time
import string
from collections import defaultdict

TARGET_TOTAL_SIZE_GB = 6.0
BASE_DIR = "/Volumes/HomeXx/compuir/texts"
WIKI_URL = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"

# File size limits (in MB)
MIN_FILE_SIZE_MB = 0.01  # 10 KB minimum
MAX_FILE_SIZE_MB = 10.0   # 10 MB maximum

# Target files per directory for even distribution
TARGET_FILES_PER_DIR = 30  # Will distribute evenly across all directories

# Directory structure parameters
MAX_DEPTH = 5
TOTAL_DIRECTORIES = 150  # Total number of directories to create
DIR_CREATION_PROBABILITY = 0.85

# File type distribution for more realistic testing
FILE_EXTENSIONS = [
    ('.txt', 0.30),
    ('.md', 0.15),
    ('.log', 0.10),
    ('.json', 0.15),
    ('.xml', 0.10),
    ('.csv', 0.10),
    ('.dat', 0.10),
]

# More realistic file size distribution (within min/max limits)
FILE_SIZE_DISTRIBUTION = [
    (0.01, 0.1, 0.25),   # 25% small files (10KB - 100KB)
    (0.1, 0.5, 0.35),    # 35% medium files (100KB - 500KB)  
    (0.5, 2.0, 0.25),    # 25% large files (500KB - 2MB)
    (2.0, 5.0, 0.10),    # 10% very large files (2MB - 5MB)
    (5.0, 10.0, 0.05),   # 5% huge files (5MB - 10MB)
]

# Constants
BYTES_PER_MB = 1024 * 1024
TARGET_TOTAL_BYTES = int(TARGET_TOTAL_SIZE_GB * 1024 * 1024 * 1024)

class DirectoryNode:
    """Track directory state including file count"""
    def __init__(self, path):
        self.path = path
        self.file_count = 0
        self.total_bytes = 0
        
    def add_file(self, size_bytes):
        self.file_count += 1
        self.total_bytes += size_bytes

def get_random_dirname():
    """Generate semi-realistic directory names"""
    templates = [
        lambda: f"project_{random.randint(100, 999)}",
        lambda: f"data_{random.choice(['archive', 'backup', 'current', 'old', 'new', 'temp', 'cache'])}",
        lambda: f"{random.choice(['docs', 'files', 'resources', 'assets', 'content', 'storage'])}_{random.randint(1, 99):02d}",
        lambda: f"{random.choice(['user', 'team', 'dept', 'group'])}_{random.choice(string.ascii_lowercase)}_{random.randint(1, 50)}",
        lambda: f"{random.randint(2020, 2025)}_{random.choice(['Q1', 'Q2', 'Q3', 'Q4', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])}",
        lambda: f"v{random.randint(1,9)}.{random.randint(0,99)}.{random.randint(0,999)}",
        lambda: f"{random.choice(['alpha', 'beta', 'gamma', 'delta', 'prod', 'dev', 'test', 'staging'])}_{random.randint(1, 200)}",
    ]
    return random.choice(templates)()

def create_balanced_directory_tree(base_path, target_dir_count):
    """Create a directory tree with a specific number of directories"""
    all_nodes = {}
    
    # Add the base directory itself as a node
    base_node = DirectoryNode(base_path)
    all_nodes[base_path] = base_node
    
    # Create a queue for breadth-first directory creation
    dir_queue = [(base_path, 0)]  # (path, depth)
    dirs_created = 1  # Count the base directory
    
    while dirs_created < target_dir_count and dir_queue:
        current_path, current_depth = dir_queue.pop(0)
        
        if current_depth >= MAX_DEPTH:
            continue
            
        # Calculate how many subdirectories to create at this level
        remaining_dirs = target_dir_count - dirs_created
        if remaining_dirs <= 0:
            break
            
        # Create subdirectories
        num_subdirs = min(random.randint(2, 6), remaining_dirs)
        
        for _ in range(num_subdirs):
            if dirs_created >= target_dir_count:
                break
                
            dir_name = get_random_dirname()
            dir_path = os.path.join(current_path, dir_name)
            
            os.makedirs(dir_path, exist_ok=True)
            
            # Create node for the new directory
            new_node = DirectoryNode(dir_path)
            all_nodes[dir_path] = new_node
            dirs_created += 1
            
            # Add to queue for potential subdirectories
            if current_depth + 1 < MAX_DEPTH:
                dir_queue.append((dir_path, current_depth + 1))
    
    return all_nodes

def setup_directories():
    """Create a balanced directory tree"""
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
        print(f"Created base directory: {BASE_DIR}")
    
    print(f"Building balanced directory tree with {TOTAL_DIRECTORIES} directories...")
    print(f"Target size: {TARGET_TOTAL_SIZE_GB} GB")
    print(f"File size range: {MIN_FILE_SIZE_MB} MB - {MAX_FILE_SIZE_MB} MB")
    
    all_nodes = create_balanced_directory_tree(BASE_DIR, TOTAL_DIRECTORIES)
    
    print(f"Created {len(all_nodes)} directories")
    
    return all_nodes

def get_next_directory_for_file(all_nodes, total_bytes_written):
    """
    Select the next directory that needs files, using round-robin approach
    to ensure even distribution based on total bytes
    """
    if not all_nodes:
        return None
    
    # Calculate target bytes per directory
    target_bytes_per_dir = TARGET_TOTAL_BYTES / len(all_nodes)
    
    # Get directories sorted by how far they are from their target
    dirs_by_need = []
    for path, node in all_nodes.items():
        bytes_needed = target_bytes_per_dir - node.total_bytes
        if bytes_needed > 0:
            dirs_by_need.append((bytes_needed, node))
    
    if not dirs_by_need:
        # All directories have reached their target, just pick the one with least bytes
        return min(all_nodes.values(), key=lambda x: x.total_bytes)
    
    # Sort by bytes needed (descending) to fill directories evenly
    dirs_by_need.sort(key=lambda x: x[0], reverse=True)
    
    # Return the directory that needs the most bytes
    return dirs_by_need[0][1]

def get_random_file_size():
    """Get a file size based on distribution, clamped to min/max limits"""
    rand = random.random()
    cumulative = 0
    
    for min_size, max_size, probability in FILE_SIZE_DISTRIBUTION:
        cumulative += probability
        if rand <= cumulative:
            # Clamp to configured limits
            min_size = max(min_size, MIN_FILE_SIZE_MB)
            max_size = min(max_size, MAX_FILE_SIZE_MB)
            size_mb = random.uniform(min_size, max_size)
            return int(size_mb * BYTES_PER_MB)
    
    # Default fallback
    size_mb = random.uniform(MIN_FILE_SIZE_MB, MAX_FILE_SIZE_MB)
    return int(size_mb * BYTES_PER_MB)

def get_random_extension():
    """Get a random file extension based on distribution"""
    rand = random.random()
    cumulative = 0
    
    for ext, probability in FILE_EXTENSIONS:
        cumulative += probability
        if rand <= cumulative:
            return ext
    
    return '.txt'

def generate_pseudo_content(size_bytes):
    """Generate pseudo-random content when we run out of Wikipedia data"""
    # Create repeating pattern with some variation
    patterns = [
        b"Lorem ipsum dolor sit amet, consectetur adipiscing elit. ",
        b"The quick brown fox jumps over the lazy dog. ",
        b"Data pattern testing 0123456789 ABCDEFGHIJKLMNOPQRSTUVWXYZ. ",
        b"Sample content for file distribution testing purposes. ",
        b"This is automatically generated content for storage testing. ",
    ]
    
    content = bytearray()
    pattern_idx = 0
    
    while len(content) < size_bytes:
        # Add pattern with slight modification
        pattern = patterns[pattern_idx % len(patterns)]
        # Add random bytes for variation
        variation = bytes([random.randint(32, 126) for _ in range(20)])
        content.extend(pattern + variation + b"\n")
        pattern_idx += 1
    
    return bytes(content[:size_bytes])

def stream_and_distribute_evenly(all_nodes):
    print(f"\nStarting data generation and distribution...")
    print(f"Target: {TARGET_TOTAL_SIZE_GB} GB evenly distributed across {len(all_nodes)} directories")
    print("Fetching initial data from Wikipedia, then generating additional content as needed...\n")

    total_bytes_written = 0
    files_created = 0
    file_size_stats = {ext: 0 for ext, _ in FILE_EXTENSIONS}
    distribution_stats = defaultdict(int)
    
    # First try to get data from Wikipedia
    wikipedia_data = bytearray()
    wikipedia_exhausted = False
    
    try:
        print("Downloading and decompressing Wikipedia data...")
        with urllib.request.urlopen(WIKI_URL) as response:
            decompressor = bz2.BZ2Decompressor()
            downloaded_mb = 0
            
            while downloaded_mb < 500:  # Limit initial download to 500MB compressed
                chunk = response.read(1024 * 1024)  # Read 1MB chunks
                if not chunk:
                    wikipedia_exhausted = True
                    break
                
                downloaded_mb += 1
                
                try:
                    decompressed_data = decompressor.decompress(chunk)
                    wikipedia_data.extend(decompressed_data)
                    
                    if downloaded_mb % 10 == 0:
                        print(f"  Downloaded {downloaded_mb} MB compressed, "
                              f"decompressed to {len(wikipedia_data) / (1024**2):.1f} MB")
                    
                except EOFError:
                    wikipedia_exhausted = True
                    break
                    
    except Exception as e:
        print(f"Warning: Could not fetch Wikipedia data: {e}")
        print("Will use generated content instead...")
        wikipedia_exhausted = True
    
    print(f"\nWikipedia data buffer: {len(wikipedia_data) / (1024**2):.1f} MB")
    print("Starting file distribution...\n")
    
    # Now distribute files
    buffer = wikipedia_data
    buffer_position = 0
    
    while total_bytes_written < TARGET_TOTAL_BYTES:
        target_size = get_random_file_size()
        
        # Get content for the file
        if buffer_position + target_size <= len(buffer):
            # We have enough Wikipedia data
            file_content = bytes(buffer[buffer_position:buffer_position + target_size])
            buffer_position += target_size
            
            # Reset buffer position if we're near the end (reuse Wikipedia data)
            if buffer_position > len(buffer) - MAX_FILE_SIZE_MB * BYTES_PER_MB:
                buffer_position = 0
        else:
            # Generate pseudo content
            if len(buffer) > 0 and buffer_position < len(buffer):
                # Use remaining Wikipedia data plus generated content
                wiki_part = bytes(buffer[buffer_position:])
                generated_part = generate_pseudo_content(target_size - len(wiki_part))
                file_content = wiki_part + generated_part
                buffer_position = 0  # Reset for next file
            else:
                # Pure generated content
                file_content = generate_pseudo_content(target_size)
        
        # Get the next directory in round-robin fashion
        target_node = get_next_directory_for_file(all_nodes, total_bytes_written)
        
        if target_node is None:
            print("Error: Could not find directory for file!")
            break
        
        # Generate filename with random extension
        extension = get_random_extension()
        timestamp = int(time.time() * 1000) % 1000000
        filename = f"data_{files_created:06d}_{timestamp}{extension}"
        filepath = os.path.join(target_node.path, filename)
        
        # Write the file
        with open(filepath, "wb") as f:
            f.write(file_content)
        
        # Update tracking
        target_node.add_file(len(file_content))
        total_bytes_written += len(file_content)
        files_created += 1
        file_size_stats[extension] += 1
        distribution_stats[target_node.file_count] += 1
        
        # Progress log
        if files_created % 100 == 0 or (files_created % 10 == 0 and files_created < 100):
            gb_written = total_bytes_written / (1024**3)
            progress_pct = (total_bytes_written / TARGET_TOTAL_BYTES) * 100
            avg_size_mb = (total_bytes_written / files_created) / BYTES_PER_MB
            
            # Calculate distribution uniformity
            bytes_per_dir = [node.total_bytes for node in all_nodes.values()]
            avg_bytes = sum(bytes_per_dir) / len(bytes_per_dir) if bytes_per_dir else 0
            
            print(f"Progress: {files_created} files | {gb_written:.2f}/{TARGET_TOTAL_SIZE_GB} GB ({progress_pct:.1f}%) | "
                  f"Avg size: {avg_size_mb:.2f} MB | Avg per dir: {avg_bytes / (1024**3):.3f} GB")

    # Print detailed statistics
    print("\n" + "="*60)
    print("COMPLETION STATISTICS")
    print("="*60)
    
    print(f"\nGeneral Stats:")
    print(f"  Total Files Created: {files_created}")
    print(f"  Total Size: {total_bytes_written / (1024**3):.2f} GB")
    if files_created > 0:
        print(f"  Average File Size: {(total_bytes_written / files_created) / BYTES_PER_MB:.2f} MB")
    print(f"  File Size Range: {MIN_FILE_SIZE_MB} MB - {MAX_FILE_SIZE_MB} MB")
    print(f"  Total Directories: {len(all_nodes)}")
    
    print(f"\nDistribution Stats:")
    dirs_with_files = [n for n in all_nodes.values() if n.file_count > 0]
    print(f"  Directories with files: {len(dirs_with_files)}")
    print(f"  Empty directories: {len(all_nodes) - len(dirs_with_files)}")
    
    # File distribution stats
    file_counts = [n.file_count for n in all_nodes.values()]
    size_per_dir_gb = [n.total_bytes / (1024**3) for n in all_nodes.values()]
    
    if file_counts:
        print(f"\nPer-Directory File Stats:")
        print(f"  Min files: {min(file_counts)}")
        print(f"  Max files: {max(file_counts)}")
        print(f"  Avg files: {sum(file_counts) / len(file_counts):.1f}")
        print(f"  Std deviation: {(sum((x - (sum(file_counts) / len(file_counts)))**2 for x in file_counts) / len(file_counts))**0.5:.2f}")
    
    if size_per_dir_gb:
        print(f"\nPer-Directory Size Stats:")
        print(f"  Min size: {min(size_per_dir_gb):.3f} GB")
        print(f"  Max size: {max(size_per_dir_gb):.3f} GB")
        print(f"  Avg size: {sum(size_per_dir_gb) / len(size_per_dir_gb):.3f} GB")
        print(f"  Std deviation: {(sum((x - (sum(size_per_dir_gb) / len(size_per_dir_gb)))**2 for x in size_per_dir_gb) / len(size_per_dir_gb))**0.5:.3f} GB")
    
    print("\nFile Type Distribution:")
    for ext, count in file_size_stats.items():
        if count > 0:
            print(f"  {ext}: {count} files")
    
    print(f"\nTop 10 Largest Directories:")
    sorted_by_size = sorted(all_nodes.values(), key=lambda x: x.total_bytes, reverse=True)[:10]
    for node in sorted_by_size:
        rel_path = os.path.relpath(node.path, BASE_DIR)
        size_gb = node.total_bytes / (1024**3)
        print(f"  {rel_path}: {node.file_count} files, {size_gb:.3f} GB")
    
    print(f"\nData location: {BASE_DIR}")

if __name__ == "__main__":
    try:
        all_nodes = setup_directories()
        stream_and_distribute_evenly(all_nodes)
    except KeyboardInterrupt:
        print("\nProcess stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")