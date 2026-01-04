import timeit
from pathlib import Path

# 1. Standard Class
class t_Directory:
    def __init__(self, path):
        self.path = path
        self.target = path.parent
        self.originals = path / "originals"

# 2. Slots Class
class t_DirectorySlots:
    __slots__ = ['path', 'target', 'originals']
    def __init__(self, path):
        self.path = path
        self.target = path.parent
        self.originals = path / "originals"

# Setup Data
p = Path("/usr/local/bin")
standard_obj = t_Directory(p)
slots_obj = t_DirectorySlots(p)
simple_var = p / "originals"  # <--- Global Simple Variable

# Iterations
ITERATIONS = 100_000_000

def test_standard_access():
    # Scenario: Accessing class attribute (Dictionary Lookup)
    obj = standard_obj
    for _ in range(ITERATIONS):
        _ = obj.originals

def test_slots_access():
    # Scenario: Accessing slots attribute (Descriptor Lookup)
    obj = slots_obj
    for _ in range(ITERATIONS):
        _ = obj.originals

def test_local_proxy():
    # Scenario: Assign to LOCAL variable first (LOAD_FAST)
    # This represents the "Best Practice" optimization
    obj = standard_obj
    local_ref = obj.originals 
    for _ in range(ITERATIONS):
        _ = local_ref

def test_simple_var():
    # Scenario: Accessing GLOBAL variable directly (LOAD_GLOBAL)
    # This mimics the "Simple Variable" from the previous test
    for _ in range(ITERATIONS):
        _ = simple_var

# Run Benchmark
print(f"Running benchmark ({ITERATIONS} internal iterations)...")
print("-" * 50)

t_std = timeit.timeit(test_standard_access, number=1)
print(f"Standard Class Access:  {t_std:.4f} seconds")

t_slots = timeit.timeit(test_slots_access, number=1)
print(f"Slots Class Access:     {t_slots:.4f} seconds")

t_local = timeit.timeit(test_local_proxy, number=1)
print(f"Local Proxy (Cached):   {t_local:.4f} seconds")

t_simple = timeit.timeit(test_simple_var, number=1)
print(f"Simple Variable (Global):{t_simple:.4f} seconds")

print("-" * 50)
print(f"Speedup (Local Proxy vs Standard): {t_std / t_local:.2f}x faster")