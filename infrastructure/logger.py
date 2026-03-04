from pathlib import Path
import logging

def get_logger(log_dir: Path, log_name: str = "excel_pipeline") -> logging.Logger:
    """
    Initializes and returns a logger with both console and file handlers.

    Args:
        log_dir (Path): Directory to store the log file.
        log_name (str): Name of the logger (default: "excel_pipeline").
    
    Returns:
    logging.Logger: Configured logger instance.
    """

    # Ensure the log directory exists
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        "(%(asctime)s) | %(name)s | %(levelname)s => '%(message)s'"
    )

    # File handler
    file_handler = logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Avoid adding handlers multiple times
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger