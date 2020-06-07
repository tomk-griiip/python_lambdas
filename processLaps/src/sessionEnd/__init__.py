import os
import logging
# add log level
logger = logging.getLogger(__name__)
logger.setLevel(int(os.environ['logLevel']))