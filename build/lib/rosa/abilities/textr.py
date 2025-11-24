"""
Downloads Wikipedia pages with realistic directory structure and file distribution.
Used for stress testing upload/download speeds with real-world-like data patterns.
Enforces strict limits on files and subdirectories per folder.
"""

import os
import random
import bz2
import urllib.request
import time
import string

TARGET_TOTAL_SIZE_GB = 22.0
BASE_DIR = "/Volumes/HomeXx/compuir/texts"
WIKI_URL = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"

# Hard limits per directory
MAX_FILES_PER_DIR = 45  # Maximum files in any single directory
MAX_SUBDIRS_PER_DIR = 6  # Maximum subdirectories in any single directory

# More realistic file size distribution (in MB)
FILE_SIZE_DISTRIBUTION = [
    (0.01, 0.1, 0.25),   # 25% small files (10KB - 100KB)
    (0.1, 0.5, 0.35),    # 35% medium files (100KB - 500KB)  
    (0.5, 2.0, 0.25),    # 25% large files (500KB - 2MB)
    (2.0, 5.0, 0.10),    # 10% very large files (2MB - 5MB)
    (5.0, 10.0, 0.05),   # 5% huge files (5MB - 10MB)
]

# Directory structure parameters
MAX_DEPTH = 5  # Maximum directory depth (increased for more directories)
MIN_SUBDIRS = 2  # Minimum subdirectories per folder (increased for more structure)
DIR_CREATION_PROBABILITY = 0.85  # Higher probability to create more subdirectories

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

# Constants
BYTES_PER_MB = 1024 * 1024
TARGET_TOTAL_BYTES = int(TARGET_TOTAL_SIZE_GB * 1024 * 1024 * 1024)

class DirectoryNode:
    """Track directory state including file count and subdirectory count"""
    def __init__(self, path):
        self.path = path
        self.file_count = 0
        self.subdir_count = 0
        self.is_full = False
        
    def can_add_file(self):
        return self.file_count < MAX_FILES_PER_DIR and not self.is_full
    
    def can_add_subdir(self):
        return self.subdir_count < MAX_SUBDIRS_PER_DIR
    
    def add_file(self):
        self.file_count += 1
        if self.file_count >= MAX_FILES_PER_DIR:
            self.is_full = True
            
    def add_subdir(self):
        self.subdir_count += 1

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

def create_directory_tree(base_node, all_nodes, current_depth=0):
    """Create a realistic nested directory structure with limits enforced"""
    
    if current_depth >= MAX_DEPTH:
        return
    
    # Decide if this directory should have subdirectories
    if current_depth > 0 and random.random() > DIR_CREATION_PROBABILITY:
        return
    
    # Calculate how many subdirectories to create (respecting the limit)
    max_possible = MAX_SUBDIRS_PER_DIR - base_node.subdir_count
    if max_possible <= 0:
        return
        
    # Weighted towards creating more directories initially
    if current_depth < 2:
        weights = [0.05, 0.10, 0.15, 0.25, 0.25, 0.20]  # Bias towards more subdirs at shallow levels
    else:
        weights = [0.30, 0.25, 0.20, 0.15, 0.07, 0.03]  # Bias towards fewer subdirs at deeper levels
    
    num_subdirs = random.choices(
        range(MIN_SUBDIRS, min(MAX_SUBDIRS_PER_DIR + 1, max_possible + MIN_SUBDIRS)),
        weights=weights[:min(MAX_SUBDIRS_PER_DIR + 1 - MIN_SUBDIRS, max_possible)]
    )[0] if max_possible > 0 else 0
    
    for _ in range(min(num_subdirs, max_possible)):
        dir_name = get_random_dirname()
        dir_path = os.path.join(base_node.path, dir_name)
        
        # Check if we can still add a subdirectory
        if not base_node.can_add_subdir():
            break
            
        os.makedirs(dir_path, exist_ok=True)
        base_node.add_subdir()
        
        # Create node for the new directory
        new_node = DirectoryNode(dir_path)
        all_nodes[dir_path] = new_node
        
        # Recursively create subdirectories
        create_directory_tree(new_node, all_nodes, current_depth + 1)

def setup_directories():
    """Create a complex, realistic directory tree with tracking"""
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
        print(f"Created base directory: {BASE_DIR}")
    
    print("Building directory tree structure with enforced limits...")
    print(f"Max files per directory: {MAX_FILES_PER_DIR}")
    print(f"Max subdirectories per directory: {MAX_SUBDIRS_PER_DIR}")
    
    # Dictionary to track all directory nodes
    all_nodes = {}
    
    # Create multiple root branches (more than before for more directories)
    root_branches = random.randint(8, 15)  # Increased from 3-8
    
    for i in range(root_branches):
        branch_name = get_random_dirname()
        branch_path = os.path.join(BASE_DIR, branch_name)
        os.makedirs(branch_path, exist_ok=True)
        
        # Create node for this branch
        branch_node = DirectoryNode(branch_path)
        all_nodes[branch_path] = branch_node
        
        # Create subdirectory tree for each branch
        create_directory_tree(branch_node, all_nodes)
    
    # Also create some standalone directories at root level for variety
    for _ in range(random.randint(5, 10)):
        standalone_name = get_random_dirname()
        standalone_path = os.path.join(BASE_DIR, standalone_name)
        os.makedirs(standalone_path, exist_ok=True)
        all_nodes[standalone_path] = DirectoryNode(standalone_path)
    
    print(f"Created {len(all_nodes)} directories in tree structure")
    return all_nodes

def get_available_directory(all_nodes):
    """Select a directory that still has room for files"""
    available_dirs = [node for node in all_nodes.values() if node.can_add_file()]
    
    if not available_dirs:
        # All directories are full, need to create new ones
        return None
    
    # Weight selection based on depth and current fill level
    weights = []
    for node in available_dirs:
        depth = node.path.count(os.sep) - BASE_DIR.count(os.sep)
        
        # Prefer directories that are partially filled (more realistic)
        fill_ratio = node.file_count / MAX_FILES_PER_DIR
        if fill_ratio < 0.3:
            weight = 2.0
        elif fill_ratio < 0.7:
            weight = 3.0  # Prefer mid-filled directories
        else:
            weight = 1.5
        
        # Adjust weight by depth (prefer mid-level directories)
        if depth == 2 or depth == 3:
            weight *= 1.5
        elif depth > 4:
            weight *= 0.7
            
        weights.append(weight)
    
    return random.choices(available_dirs, weights=weights)[0]

def create_overflow_directory(all_nodes):
    """Create a new directory when all existing ones are full"""
    # Find a directory that can still have subdirectories
    potential_parents = [node for node in all_nodes.values() if node.can_add_subdir()]
    
    if not potential_parents:
        print("Warning: Cannot create more directories, all parent directories at subdir limit")
        return None
    
    parent = random.choice(potential_parents)
    dir_name = f"overflow_{len(all_nodes):04d}"
    dir_path = os.path.join(parent.path, dir_name)
    os.makedirs(dir_path, exist_ok=True)
    
    parent.add_subdir()
    new_node = DirectoryNode(dir_path)
    all_nodes[dir_path] = new_node
    
    return new_node

def get_random_file_size():
    """Get a file size based on realistic distribution"""
    rand = random.random()
    cumulative = 0
    
    for min_size, max_size, probability in FILE_SIZE_DISTRIBUTION:
        cumulative += probability
        if rand <= cumulative:
            size_mb = random.uniform(min_size, max_size)
            return int(size_mb * BYTES_PER_MB)
    
    return int(0.5 * BYTES_PER_MB)

def get_random_extension():
    """Get a random file extension based on distribution"""
    rand = random.random()
    cumulative = 0
    
    for ext, probability in FILE_EXTENSIONS:
        cumulative += probability
        if rand <= cumulative:
            return ext
    
    return '.txt'

def stream_and_scatter(all_nodes):
    print(f"\nStarting stream from: {WIKI_URL}")
    print(f"Target: {TARGET_TOTAL_SIZE_GB} GB with realistic file distribution")
    print("This may take time depending on your internet connection...\n")

    total_bytes_written = 0
    files_created = 0
    overflow_dirs_created = 0
    file_size_stats = {ext: 0 for ext, _ in FILE_EXTENSIONS}
    
    # Connect to the URL
    with urllib.request.urlopen(WIKI_URL) as response:
        decompressor = bz2.BZ2Decompressor()
        buffer = b""
        
        while total_bytes_written < TARGET_TOTAL_BYTES:
            # Read a chunk from the network
            chunk = response.read(1024 * 64)  # Read 64KB chunks
            if not chunk:
                break
            
            # Decompress the chunk and add to buffer
            try:
                decompressed_data = decompressor.decompress(chunk)
                buffer += decompressed_data
            except EOFError:
                break

            # Process buffer when we have enough data
            while True:
                target_size = get_random_file_size()
                
                if len(buffer) < target_size:
                    break  # Need more data
                
                # Slice off the chunk for the file
                file_content = buffer[:target_size]
                buffer = buffer[target_size:]
                
                # Get an available directory
                target_node = get_available_directory(all_nodes)
                
                if target_node is None:
                    # All directories are full, create overflow directory
                    target_node = create_overflow_directory(all_nodes)
                    if target_node:
                        overflow_dirs_created += 1
                        print(f"Created overflow directory #{overflow_dirs_created} (all existing dirs approaching capacity)")
                    else:
                        print("ERROR: Cannot create more directories or files - all at capacity!")
                        break
                
                if target_node is None:
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
                target_node.add_file()
                total_bytes_written += len(file_content)
                files_created += 1
                file_size_stats[extension] += 1
                
                # Progress log
                if files_created % 100 == 0:
                    gb_written = total_bytes_written / (1024**3)
                    avg_size_mb = (total_bytes_written / files_created) / BYTES_PER_MB
                    dirs_with_files = len([n for n in all_nodes.values() if n.file_count > 0])
                    full_dirs = len([n for n in all_nodes.values() if n.is_full])
                    print(f"Progress: {files_created} files | {gb_written:.2f}/{TARGET_TOTAL_SIZE_GB} GB | "
                          f"Dirs used: {dirs_with_files} | Full dirs: {full_dirs} | Avg size: {avg_size_mb:.2f} MB")
                
                if total_bytes_written >= TARGET_TOTAL_BYTES:
                    break

    # Print detailed statistics
    print("\n" + "="*60)
    print("COMPLETION STATISTICS")
    print("="*60)
    
    print(f"\nGeneral Stats:")
    print(f"  Total Files Created: {files_created}")
    print(f"  Total Size: {total_bytes_written / (1024**3):.2f} GB")
    print(f"  Average File Size: {(total_bytes_written / files_created) / BYTES_PER_MB:.2f} MB")
    print(f"  Total Directories: {len(all_nodes)}")
    print(f"  Overflow Directories Created: {overflow_dirs_created}")
    
    print(f"\nDirectory Usage Stats:")
    dirs_with_files = [n for n in all_nodes.values() if n.file_count > 0]
    print(f"  Directories with files: {len(dirs_with_files)}")
    print(f"  Empty directories: {len(all_nodes) - len(dirs_with_files)}")
    print(f"  Directories at file limit ({MAX_FILES_PER_DIR} files): {len([n for n in all_nodes.values() if n.is_full])}")
    
    # File distribution stats
    file_counts = [n.file_count for n in dirs_with_files]
    if file_counts:
        print(f"  Min files in a directory: {min(file_counts)}")
        print(f"  Max files in a directory: {max(file_counts)}")
        print(f"  Avg files per directory: {sum(file_counts) / len(file_counts):.1f}")
    
    print("\nFile Type Distribution:")
    for ext, count in file_size_stats.items():
        if count > 0:
            print(f"  {ext}: {count} files")
    
    print("\nTop 10 Most Populated Directories:")
    sorted_nodes = sorted(dirs_with_files, key=lambda x: x.file_count, reverse=True)[:10]
    for node in sorted_nodes:
        rel_path = os.path.relpath(node.path, BASE_DIR)
        print(f"  {rel_path}: {node.file_count} files")
    
    print(f"\nData location: {BASE_DIR}")

if __name__ == "__main__":
    try:
        all_nodes = setup_directories()
        stream_and_scatter(all_nodes)
    except KeyboardInterrupt:
        print("\nProcess stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")