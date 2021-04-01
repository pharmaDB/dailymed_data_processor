import logging

_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def getLogger(name):
    # Set up console log handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(_FORMAT))

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(ch)
    return logger
