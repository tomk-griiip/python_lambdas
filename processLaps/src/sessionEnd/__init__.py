import os
import logging
import watchtower
# add log level
logger = logging.getLogger(__name__)
logger.addHandler(watchtower.CloudWatchLogHandler())
try:
    logger.setLevel(int(os.environ['logLevel']))
except ValueError:
    logger.setLevel(10)