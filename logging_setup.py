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


# Holds the real console stream (captured before stdout is redirected to the
# logger) so banners and progress lines can be written back to the terminal.
_console_stream = None

# Message prefixes emitted by orca as it runs each iteration/step. These are the
# "forest" lines we surface to the console and turn into log-file chapters.
_ORCA_PROGRESS_PREFIXES = (
    "Running step ",
    "Time to execute step ",
    "Running iteration ",
    "Total time to execute iteration ",
)


def _current_year():
    """Returns the current simulation year from orca's "year" injectable.

    Args:
        None.

    Returns:
        The current iteration year (int). Outside a simulation run, BAUS's
        "year" injectable falls back to its base year.
    """
    import orca
    return orca.get_injectable("year")


def _format_duration(seconds):
    """Formats an elapsed-seconds value, switching to minutes past 60 seconds.

    Args:
        seconds: The elapsed time in seconds.

    Returns:
        A human-readable duration string: "<m>m <s>s" when longer than 60
        seconds, otherwise "<seconds>s" with two decimal places.

    Example:
        >>> _format_duration(561.02)
        '9m 21s'
        >>> _format_duration(42.5)
        '42.50s'
    """
    if seconds > 60:
        whole_minutes = int(seconds // 60)
        remaining_seconds = int(round(seconds - whole_minutes * 60))
        return "{m}m {s}s".format(m=whole_minutes, s=remaining_seconds)
    return "{s:.2f}s".format(s=seconds)


class ProgressConsoleFilter(logging.Filter):
    """Allows only high-level progress lines and banners through to the console.

    The full detailed log still goes to the file. This filter keeps the terminal
    readable by passing only orca's per-iteration/per-step progress lines and any
    record explicitly flagged as a banner.
    """

    def filter(self, record):
        """Decides whether a log record should be shown on the console.

        Args:
            record: The logging.LogRecord being evaluated.

        Returns:
            True if the record is a banner or an orca progress line, else False.
        """
        if getattr(record, "baus_banner", False):
            return True
        message = record.getMessage()
        return any(message.startswith(prefix) for prefix in _ORCA_PROGRESS_PREFIXES)


class ChapterFileFormatter(logging.Formatter):
    """Formats orca step-start lines as visual chapter separators in the log file.

    Every "Running step '<name>'" line orca prints is rendered as a boxed
    separator so the otherwise-dense log can be scanned like a book with chapters.
    All other records use the standard configured format.
    """

    _BAR = "=" * 78

    def format(self, record):
        """Formats a log record, boxing orca step-start lines as chapters.

        Args:
            record: The logging.LogRecord to format.

        Returns:
            The formatted log string. Step-start lines become a three-line boxed
            chapter header; all other records use the standard format.
        """
        message = record.getMessage()
        if message.startswith("Running step "):
            step_name = message[len("Running step "):].strip().strip("'\"")
            timestamp = self.formatTime(record, self.datefmt)
            return "\n{bar}\n{ts}  >>> STEP: {step}\n{bar}".format(
                bar=self._BAR, ts=timestamp, step=step_name)
        return super(ChapterFileFormatter, self).format(record)


class ConsoleProgressFormatter(logging.Formatter):
    """Renders orca progress lines as concise, year-tagged console output.

    Turns each orca "Running step '<name>'" line into a readable
    "<year>  >  <step>" line and each "Time to execute step" line into an
    indented timing note (minutes past 60 seconds). Banner records pass through
    unformatted.
    """

    def format(self, record):
        """Formats a progress or banner record for the console.

        Args:
            record: The logging.LogRecord to format.

        Returns:
            The console string: banners verbatim, step-start lines tagged with
            the current simulation year, timing lines indented, and any other
            progress line passed through unchanged.
        """
        if getattr(record, "baus_banner", False):
            return record.getMessage()

        message = record.getMessage()

        if message.startswith("Running step "):
            step_name = message[len("Running step "):].strip().strip("'\"")
            return "  {year}   >  {step}".format(year=_current_year(), step=step_name)

        if message.startswith("Time to execute step "):
            timing = message.split(": ", 1)[-1].strip()
            seconds = float(timing.rstrip("s"))
            return "            done in {duration}".format(duration=_format_duration(seconds))

        return message


def setup_logging(log_file, log_level=logging.INFO, detail_level="medium"):
    """Sets up file + console logging and redirects stdout/stderr to the logger.

    Configures a detailed file log (with orca step lines rendered as chapter
    separators) and a concise console stream that surfaces only high-level
    progress lines and banners. stdout/stderr are redirected into the file logger
    so stray prints are still captured.

    Args:
        log_file: Path to the log file to write the detailed log to.
        log_level: Logging level threshold (e.g. logging.INFO).
        detail_level: One of "low", "medium", or "high", controlling the file
            log format verbosity.

    Returns:
        The configured "baus" logging.Logger instance.

    See Also:
        log_banner: Emits boxed section banners to both the file log and console.
    """
    global _console_stream

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

    # Render orca step lines as chapter separators in the file log.
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.setFormatter(ChapterFileFormatter(fmt=format_string, datefmt='%Y-%m-%d %H:%M:%S'))

    # Capture the real console before stdout is redirected, then attach a console
    # handler that only shows high-level progress lines and banners.
    _console_stream = sys.stdout
    console_handler = logging.StreamHandler(stream=_console_stream)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(ConsoleProgressFormatter())
    console_handler.addFilter(ProgressConsoleFilter())
    root_logger.addHandler(console_handler)

    # Redirect stdout and stderr to the logger, capturing print statements for now
    sys.stdout = StreamToLogger(logging.getLogger('baus'), logging.INFO)
    sys.stderr = StreamToLogger(logging.getLogger('baus'), logging.ERROR)

    return logger


def log_banner(title, subtitle=None, char="=", width=78, logger=None):
    """Emits a large, boxed banner to both the log file and the console.

    Use this to mark major phases of a run (e.g. base-year pass, simulation pass)
    so the log reads like a book with chapters and the console shows a clear,
    legible progress trail.

    Args:
        title: The main banner text, centered on its own line.
        subtitle: Optional secondary line, centered beneath the title.
        char: The character used to draw the top and bottom border rules.
        width: The total width, in characters, of the banner.
        logger: The logging.Logger to emit through. Defaults to the "baus" logger.

    Returns:
        None.

    Example:
        >>> log_banner("SIMULATION PASS", "years 2015-2050 | 42 models")

    See Also:
        setup_logging: Installs the console handler that renders these banners.
    """
    active_logger = logger if logger is not None else logging.getLogger("baus")
    border = char * width
    lines = [border, title.center(width)]
    if subtitle is not None:
        lines.append(subtitle.center(width))
    lines.append(border)
    banner_text = "\n".join(lines)
    active_logger.info(banner_text, extra={"baus_banner": True})

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