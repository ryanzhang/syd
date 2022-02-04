import os
from jproperties import Properties

configs = Properties()

with open('syd/resources/app-config.properties', 'rb') as config_file:
    configs.load(config_file)
    if configs.get("cache_folder") is not None:
        cache_folder = configs["cache_folder"].data
        if not os.path.exists(cache_folder):
            os.makedirs(cache_folder, 755)

    if configs.get("log_output_path") is not None:
        log_file_path = configs["log_output_path"].data
        log_filename = os.path.basename(log_file_path)
        log_folder =  log_file_path.replace(log_filename, '')
        if not os.path.exists(log_folder):
            os.makedirs(log_folder, 755)


