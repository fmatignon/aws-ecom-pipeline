"""
Provides UTC timestamped logging helpers that satisfy workspace logging rules.
"""

from datetime import datetime, UTC
from typing import Optional


def _utc_timestamp() -> str:
    """
    Generate the current UTC timestamp string.

    Returns:
        str: Timestamp formatted as YYYY-MM-DD HH:MM:SS in UTC.
    """
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def log_section_start(section: str) -> None:
    """
    Log the start of a section using the required format.

    Args:
        section (str): Description of the section that is beginning.
    """
    print(f"[{_utc_timestamp()}] Starting: {section}")


def log_section_complete(section: str, details: Optional[str] = None) -> None:
    """
    Log the completion of a section using the required format.

    Args:
        section (str): Description of the section that finished.
        details (Optional[str]): Optional extra context to append to the message.
    """
    suffix = f" - {details}" if details else ""
    print(f"[{_utc_timestamp()}] Completed: {section}{suffix}")


def log_progress(
    section: str,
    message: str,
    *,
    end: str = "\n",
    flush: bool = False,
) -> None:
    """
    Log an in-progress update for a section.

    Args:
        section (str): Description of the section that is running.
        message (str): Progress message to display for the section.
        end (str): Print function end parameter for controlling newline behavior.
        flush (bool): Whether to force flush the output buffer.
    """
    print(f"[{_utc_timestamp()}] {section}: {message}", end=end, flush=flush)


def log_error(section: str, error: Exception | str) -> None:
    """
    Log an error that occurred during a section.

    Args:
        section (str): Description of the section where the error occurred.
        error (Exception | str): Exception instance or error message to record.
    """
    print(f"[{_utc_timestamp()}] Error in {section}: {error}")


def clear_progress_line(section: str) -> None:
    """
    Clear the most recent progress line for a section.

    Args:
        section (str): Description of the section whose progress output should be removed.
    """
    log_progress(section, "Clearing progress line", end="\r\033[K", flush=True)
