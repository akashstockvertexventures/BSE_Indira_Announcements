import logging
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler
from config.settings import LOG_DIR


def get_logger(name: str = "news_pipeline", save_time_logs: bool = True) -> logging.Logger:
    """
    Create a logger. If save_time_logs=True -> rotate daily.
    If False -> single static log file with no rotation.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    file_name = f"{name}.log"
    log_path = LOG_DIR / file_name

    # choose handler based on flag
    if save_time_logs:
        handler = ConcurrentTimedRotatingFileHandler(
            filename=str(log_path),
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8",
        )
    else:
        handler = logging.FileHandler(str(log_path), encoding="utf-8")

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # console handler for dev
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    return logger
