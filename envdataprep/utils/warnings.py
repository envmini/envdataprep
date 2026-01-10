"""Standardized warning system for envdataprep operations.

Environmental data comes from diverse sources with varying conventions and 
encoding practices. This module provides consistent, actionable warnings to 
help users validate data operations and handle edge cases across different 
formats (NetCDF, GRIB2, TIFF, etc.).
"""

import threading

# Thread-local storage for tracking warnings per process/thread
_thread_local = threading.local()


def _get_shown_warnings():
    """Get the shown warnings set for current thread/process."""
    if not hasattr(_thread_local, 'shown_warnings'):
        _thread_local.shown_warnings = set()
    return _thread_local.shown_warnings


def warn_unvalidated_subset(
    *,
    data_format: str,
    validate_subset: bool = False,
    warnings_enabled: bool = True,
):
    """
    Emit a standardized warning when subsetting is performed without
    validation. Only shows once per process to avoid cluttering output.

    Parameters
    ----------
    data_format : str
        Name of the data format (e.g., 'netCDF', 'GRIB2', 'HDF').
    validate_subset : bool
        Whether automatic subset validation is enabled.
    warnings_enabled : bool
        Whether user-facing warnings are enabled.
    """
    if not warnings_enabled or validate_subset:
        return

    # Create a unique key for this warning type
    warning_key = f"unvalidated_subset_{data_format}"

    # Get thread-local warnings set
    shown_warnings = _get_shown_warnings()

    # Only show if we haven't shown this warning before in this process
    if warning_key in shown_warnings:
        return

    # Mark this warning as shown
    shown_warnings.add(warning_key)

    print(
        f"⚠️  {data_format} Validation Warning (shown once per process)\n"
        f"{data_format} data are provided by multiple sources with "
        "heterogeneous conventions, metadata completeness, and encoding "
        "practices.\n"
        "Subsetting is performed using standardized rules, but automatic "
        "validation cannot exhaustively cover all provider-specific edge "
        "cases.\n"
        "\n"
        "Recommendations:\n"
        " • Enable 'validate_subset=True' to perform built-in validation.\n"
        " • Perform an independent sanity check when working with a new "
        "   data provider or unfamiliar dataset.\n"
        " • Set 'warnings_enabled=False' to suppress this message.\n"
    )


def reset_warning_state():
    """Reset warning state for current process to show all warnings again.
    
    Useful for testing or when you want to see warnings again
    in the same process.
    """
    shown_warnings = _get_shown_warnings()
    shown_warnings.clear()


def get_shown_warnings():
    """Get list of warnings that have been shown in current process.
    
    Returns
    -------
    set
        Set of warning keys that have been displayed in this process.
    """
    return _get_shown_warnings().copy()
