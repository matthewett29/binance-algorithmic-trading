import configparser


# create config parser
def get_config():
    try:
        config = configparser.ConfigParser()
        config.read('app.cfg')
    except configparser.Error:
        return None
    else:
        return config
