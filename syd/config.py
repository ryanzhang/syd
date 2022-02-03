from jproperties import Properties

configs = Properties()

with open('syd/resources/app-config.properties', 'rb') as config_file:
    configs.load(config_file)


