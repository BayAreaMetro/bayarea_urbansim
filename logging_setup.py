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

def setup_logging(log_file, log_level=logging.INFO):
    """Set up logging and redirect stdout/stderr to the logger."""
    logger = logging.getLogger('baus')  # Set up a named logger for the main BAUS module

    # Logging configuration
    logging.basicConfig(
        filename=log_file,
        level=log_level,  # Default level passed to the logger
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Redirect stdout and stderr to the logger, capturing print statements for now
    sys.stdout = StreamToLogger(logging.getLogger('baus'), logging.INFO)
    sys.stderr = StreamToLogger(logging.getLogger('baus'), logging.ERROR)

    return logger
