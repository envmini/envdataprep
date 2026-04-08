"""Utility functions for I/O."""

import os
import numpy as np


def build_output_path(
    input_path: str,
    output_dir: str | None = None,
    custom_name: str | None = None,
    suffix: str | None = "_SUB",
) -> str:
    """Build output file path from input path.

    Parameters
    ----------
    input_path : str
        Path to input file.
    output_dir : str, optional
        Output directory. If None, uses the input file's directory.
    custom_name : str, optional
        Custom filename (takes priority over other options).
    suffix : str or None, default "_SUB"
        Suffix to append before the file extension.
        If None, uses the original filename unchanged.

    Returns
    -------
    str
        Full output file path.
    """
    if custom_name:
        filename = custom_name
    elif suffix:
        base = os.path.splitext(os.path.basename(input_path))[0]
        ext = os.path.splitext(input_path)[1]
        filename = f"{base}{suffix}{ext}"
    else:
        filename = os.path.basename(input_path)

    if output_dir is None:
        output_dir = os.path.dirname(input_path)

    # Make sure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    return os.path.join(output_dir, filename)


def report_size_reduction(original_path: str, output_path: str) -> float:
    """Report the size reduction of an output file compared to the original file.

    Parameters
    ----------
    original_path : str
        Path to original file.
    output_path : str
        Path to output file.

    Returns
    -------
    float
        Size reduction of the output file compared to the original file.
    """
    try:
        original_size = os.path.getsize(original_path)
        output_size = os.path.getsize(output_path)
        size_reduction = (original_size - output_size) / original_size * 100
        return np.round(size_reduction, 2)
    except (OSError, ZeroDivisionError) as e:
        raise OSError(f"Could not report file size reduction: {e}") from e
