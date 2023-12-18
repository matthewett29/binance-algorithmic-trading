import logging


class Logger():
    """
    Docstring
    """
    logger = None
    logger_name = None
    log_level = None

    def __init__(self, logger_name, log_level):

        # create logger
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        logger.propagate = True

        # create formatter
        formatter = logging.Formatter('%(asctime)s %(filename)20s %(funcName)20s %(levelname)8s: %(message)s')
        
        # create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)

        # create file handler
        fh = logging.FileHandler('logs/app.log')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)

        # add console and file handlers to the logger
        logger.addHandler(ch)
        logger.addHandler(fh)

        self.logger = logger
        self.logger_name = logger_name
        self.logger_level = log_level

    def get_logger(self):
        return self.logger


        