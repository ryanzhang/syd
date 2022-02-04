import logging
from syd.config  import configs

level=configs["log_level"].data
logging_level={"CRITICAL":logging.CRITICAL,"ERROR":logging.ERROR, \
    "WARNING":logging.WARNING, "INFO":logging.INFO, "DEBUG":logging.DEBUG}[level]

if configs.get("log_output_path") is not None and configs.get("log_output_path") != "":
    logging.basicConfig(
        filename=configs["log_output_path"].data, filemode='a', level=logging_level, \
            format=" %(asctime)s - %(module)s - %(levelname)s- %(message)s"
    )
else:
    logging.basicConfig(
        level=logging_level, format=" %(asctime)s - %(module)s - %(levelname)s- %(message)s"
    ) 

logger=logging
