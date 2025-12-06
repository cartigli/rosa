import os
import sys
from pathlib import Path

if __name__=="__main__":
    cd = Path(__file__)
    rosa_ = cd.parent.parent
    if rosa_ not in sys.path:
        sys.path.insert(0, rosa_)
    
    from rosa.configurables.config import *