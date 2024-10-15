import logging

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        filename='application.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
