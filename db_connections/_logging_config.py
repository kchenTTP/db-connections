import logging


def _get_logger(level: int = logging.INFO):
    format = "%(levelname).8s %(name).12s %(asctime)s %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=level, format=format, datefmt=datefmt)

    logger = logging.getLogger(__name__)
    return logger
