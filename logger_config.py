import logging
import sys

def setup_logger():
    """
    Configures the root logger for the application.
    """
    # Get the root logger
    logger = logging.getLogger()
    
    # Set the lowest logging level to capture all messages
    logger.setLevel(logging.INFO)

    # Prevent adding multiple handlers if this function is called more than once
    if not logger.handlers:
        # Create a handler to output logs to the console (standard output)
        handler = logging.StreamHandler(sys.stdout)
        
        # Create a formatter to define the log message structure
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Set the formatter for the handler
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        logger.addHandler(handler)
