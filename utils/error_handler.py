# File: src/utils/error_handler.py
from .logger import log_error  # Relative import

def handle_api_error(exception, module_name):
    """
    Handle API-related errors and log them.
    Args:
        exception (Exception): The caught exception.
        module_name (str): Name of the module where the error occurred.
    Raises:
        Exception: Re-raises the exception after logging.
    """
    error_data = {
        "module": module_name,
        "error": f"API Error: {str(exception)}"
    }
    log_error(error_data)
    raise

def handle_data_error(exception, module_name):
    """
    Handle data-related errors (e.g., missing columns, invalid types).
    Args:
        exception (Exception): The caught exception.
        module_name (str): Name of the module where the error occurred.
    Raises:
        Exception: Re-raises the exception after logging.
    """
    error_data = {
        "module": module_name,
        "error": f"Data Error: {str(exception)}"
    }
    log_error(error_data)
    raise

def handle_general_error(exception, module_name):
    """
    Handle generic errors not covered by other handlers.
    Args:
        exception (Exception): The caught exception.
        module_name (str): Name of the module where the error occurred.
    Raises:
        Exception: Re-raises the exception after logging.
    """
    error_data = {
        "module": module_name,
        "error": f"General Error: {str(exception)}"
    }
    log_error(error_data)
    raise

if __name__ == "__main__":
    # Test error handling
    try:
        raise ValueError("Test error")
    except ValueError as e:
        handle_data_error(e, "test")