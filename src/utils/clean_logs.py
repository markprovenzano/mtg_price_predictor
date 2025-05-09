# src/utils/clean_logs.py
import os
import glob
from datetime import datetime
from src.utils.logger import logger  # Absolute import


def clean_logs(log_dir: str = None, keep_latest: bool = True):
    """
    Remove outdated .log files from the log directory, optionally keeping the latest.

    Args:
        log_dir (str): Path to log directory (default: logs/ in project root).
        keep_latest (bool): If True, keep the most recent .log file.

    Returns:
        bool: True if successful, False if an error occurs.
    """
    try:
        # Default to logs/ in project root
        if log_dir is None:
            PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            log_dir = os.path.join(PROJECT_ROOT, "logs")

        logger.info(f"Cleaning log files in {log_dir}")
        log_files = glob.glob(os.path.join(log_dir, "mtg_price_predictor_*.log"))
        if not log_files:
            logger.info("No log files found to clean")
            return True

        if keep_latest:
            # Find the latest file by modification time
            latest_file = max(log_files, key=os.path.getmtime)
            log_files.remove(latest_file)
            logger.info(f"Keeping latest log file: {latest_file}")

        # Delete outdated files
        for log_file in log_files:
            os.remove(log_file)
            logger.info(f"Deleted outdated log file: {log_file}")

        logger.info("Log cleanup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to clean logs: {e}")
        return False


if __name__ == "__main__":
    clean_logs()