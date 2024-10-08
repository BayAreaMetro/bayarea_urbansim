import logging
import sys

# Add this hack to capture stdout and sterr printouts to the logger
class StreamToLogger(object):
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line)

    def flush(self):
        pass

def setup_logging(log_file):
    # Configure logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # File handler to write logs to a file
    file_handler = logging.FileHandler(log_file, 'w')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Redirect stdout and stderr to the logger
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr = StreamToLogger(logger, logging.ERROR)

    return logger

