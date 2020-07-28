import os
import logging
import watchtower
# add log level
logger = logging.getLogger(__name__)
logger.addHandler(watchtower.CloudWatchLogHandler())
logger.setLevel(int(os.environ['logLevel']))