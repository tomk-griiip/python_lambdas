import inspect
import os
import sys


# add src to sys path
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, f"{current_dir}")
sys.path.insert(1, f"{parent_dir}")
print(sys.path)
