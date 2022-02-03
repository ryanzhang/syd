import logging
from syd.config  import configs

level=configs["logging_level"].data
logging_level={"CRITICAL":logging.CRITICAL,"ERROR":logging.ERROR, \
    "WARNING":logging.WARNING, "INFO":logging.INFO, "DEBUG":logging.DEBUG}[level]

logging.basicConfig(
    level=logging_level, format=" %(asctime)s - %(module)s - %(levelname)s- %(message)s"
)
logger=logging
