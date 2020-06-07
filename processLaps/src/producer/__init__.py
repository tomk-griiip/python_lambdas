import os
import sys
import inspect
import logging
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, f"{current_dir}")
sys.path.insert(1, f"{parent_dir}")
# add log level
logger = logging.getLogger(__name__)
logger.setLevel(int(os.environ['logLevel']))