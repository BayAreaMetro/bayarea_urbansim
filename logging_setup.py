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
        filemode='w',     # Don't append - create new log
        level=log_level,  # Default level passed to the logger
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Redirect stdout and stderr to the logger, capturing print statements for now
    sys.stdout = StreamToLogger(logging.getLogger('baus'), logging.INFO)
    sys.stderr = StreamToLogger(logging.getLogger('baus'), logging.ERROR)

    logger.debug("baus logger:{}".format(logger))
    # set orca logger to warn; info and debug aren't useful
    logging.getLogger('orca').setLevel(logging.WARN)
    logger.debug("orca logger:{}".format(logging.getLogger('orca')))
    # set these to info
    logging.getLogger('urbansim.models.util').setLevel(logging.INFO)
    logger.debug("urbansim.models.util logger:{}".format(logging.getLogger('urbansim.models.util')))

    return logger
