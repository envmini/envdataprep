"""Utility functions for I/O."""

import os
import numpy as np


def build_subset_path(
    input_path: str,
    output_dir: str | None = None,
    output_name: str | None = None,
    use_input_name: bool = False,
    suffix: str = "_SUB",
) -> str:
    """Build subset file path from input path.

    Parameters
    ----------
    input_path : str
        Path to input file.
    output_dir : str, optional
        Output directory. If None, uses the input file's directory.
    output_name : str, optional
        Output filename (takes priority over other options).
    use_input_name : bool, default True
        If True, uses the input file's name as the output file's name.
        If False, uses the output name as the output file's name.
    suffix : str, default "_SUB"
        Suffix to append before the file extension.
        This is the default if the user does not specify an output name.
    """
    # Make sure the output directory exists
    if output_dir is None:
        output_dir = os.path.dirname(input_path)
    else:
        os.makedirs(output_dir, exist_ok=True)

    # Determine the output filename
    if output_name:
        filename = output_name
    elif use_input_name:
        filename = os.path.basename(input_path)
    else:
        base = os.path.splitext(os.path.basename(input_path))[0]
        extension = os.path.splitext(input_path)[1]
        filename = f"{base}{suffix}{extension}"
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
        return np.round(size_reduction, 1)
    except (OSError, ZeroDivisionError) as e:
        raise OSError(f"Could not report file size reduction: {e}") from e
