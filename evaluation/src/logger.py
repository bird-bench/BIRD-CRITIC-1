import logging


def configure_logger(log_filename):
    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.INFO)

    # Clear any existing handlers (in case of repeated calls)
    if logger.handlers:
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def log_section_header(section_title, logger):
    separator = f"{'=' * 20} {section_title} {'=' * 20}"
    logger.info(f"\n\n{separator}\n")


def log_section_footer(logger):
    separator = f"{'=' * 60}"
    logger.info(f"\n\n{separator}\n")


class NullLogger:
    """A Logger implementation that does not output any logs"""

    def info(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


class PrintLogger:
    """A Logger implementation that prints messages to stdout."""

    def info(self, msg, *args, **kwargs):
        print(f"[INFO] {msg}", *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        print(f"[ERROR] {msg}", *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        print(f"[WARNING] {msg}", *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        print(f"[DEBUG] {msg}", *args, **kwargs)
