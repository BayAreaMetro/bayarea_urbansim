import logging
import sys
import pandas as pd

# Add this hack to capture stdout and sterr printouts to the logger
class StreamToLogger:
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            # note as [p] so we can convert to logger lines
            self.logger.log(self.log_level, "[p] " + line)

    def flush(self):
        pass  # Required for compatibility

def setup_logging(log_file, log_level=logging.INFO):
    """Set up logging and redirect stdout/stderr to the logger."""

    # make this wide since we are logging
    pd.set_option('display.max_colwidth', 300)

    logger = logging.getLogger('baus')  # Set up a named logger for the main BAUS module

    # Logging configuration
    logging.basicConfig(
        filename=log_file,
        filemode='w',     # Don't append - create new log
        level=log_level,  # Default level passed to the logger
        # format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', # temp for debugging
        format='%(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Redirect stdout and stderr to the logger, capturing print statements for now
    sys.stdout = StreamToLogger(logging.getLogger('baus'), logging.DEBUG)  # current print is typically debug-level
    sys.stderr = StreamToLogger(logging.getLogger('baus'), logging.ERROR)

    logger.debug("baus logger:{}".format(logger))
    # set these to info
    for logger_name in [
        'orca',
        'urbansim.models.util', 
        'urbansim.models.supplydemand',
        'urbansim.models.dcm',
        'urbansim.models.regression',
        'urbansim.models.transition',
        'urbansim.urbanchoice.interaction']:

        logging.getLogger(logger_name).setLevel(logging.INFO)
        logger.debug("logger:{}".format(logging.getLogger(logger_name)))

    return logger
