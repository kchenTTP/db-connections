import logging

LEVEL = logging.DEBUG


def _get_logger(level: int = logging.INFO) -> logging.Logger:
    format = "%(levelname).8s %(name).12s %(asctime)s %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=LEVEL, format=format, datefmt=datefmt)

    logger = logging.getLogger(__name__)
    return logger
