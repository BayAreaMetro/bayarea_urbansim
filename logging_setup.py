import logging
import sys

# Add this hack to capture stdout and sterr printouts to the logger
class StreamToLogger:
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line)

    def flush(self):
        pass  # Required for compatibility

def setup_logging(log_file, log_level=logging.INFO, detail_level="medium"):
    """Set up logging and redirect stdout/stderr to the logger."""
    logger = logging.getLogger('baus')

    format_strings = {
        "low": '%(asctime)s - %(levelname)s - %(message)s',
        "medium": '%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s',
        "high": '%(asctime)s - %(module)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s',
        #"high": '%(asctime)s - %(pathname)s - %(module)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s'
    }

    format_string = format_strings.get(detail_level, format_strings["medium"])

    logging.basicConfig(
        filename=log_file,
        level=log_level,
        format=format_string,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Redirect stdout and stderr to the logger, capturing print statements for now
    sys.stdout = StreamToLogger(logging.getLogger('baus'), logging.INFO)
    sys.stderr = StreamToLogger(logging.getLogger('baus'), logging.ERROR)

    return logger

def get_log_level(level_name):
    """Converts a log level name (string) to a logging constant."""
    levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    return levels.get(level_name.upper(), logging.INFO) # we just default to info if not found.