"""Utility functions for I/O"""

import os
from typing import Optional

# TODO: Test whether @handle_file_errors decorator works correctly

def handle_file_errors(file_format: str = "file"):
    """Decorator to handle common file I/O errors with clear messages.
    
    Parameters
    ----------
    file_format : str, default "file"
        File format name for error messages (e.g., "netCDF", "TIFF", "GRIB2").
        
    Returns
    -------
    decorator
        Decorator function that wraps file I/O operations.
        
    Examples
    --------
    >>> @handle_file_errors("netCDF")
    >>> def read_netcdf(file_path):
    ...     # file reading logic
    ...     pass
    """
    def decorator(func):
        def wrapper(file_path: str, *args, **kwargs):
            try:
                return func(file_path, *args, **kwargs)
            except FileNotFoundError as exc:
                raise FileNotFoundError(
                    f"{file_format} file not found: {file_path}"
                ) from exc
            except PermissionError as exc:
                raise PermissionError(
                    f"Permission denied accessing file: {file_path}"
                ) from exc
            except (OSError, IOError) as exc:
                raise OSError(
                    f"Cannot open '{file_path}' as {file_format}"
                ) from exc
        return wrapper
    return decorator


def generate_subset_output_path(
    input_path: str,
    output_dir: Optional[str] = None,
    custom_name: Optional[str] = None
) -> str:
    """Generate output path for the subset.

    Parameters
    ----------
    input_path : str
        Path to input file.
    output_dir : str, optional
        Output directory path. If None, uses the input file's directory.
    custom_name : str, optional
        Custom filename to use instead of generating one.

    Returns
    -------
    str
        Full output file path.
    """
    if custom_name:
        filename = custom_name
    else:
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        extension = os.path.splitext(input_path)[1]
        filename = f"{base_name}_SUB{extension}"

    # Use input file's directory if output_dir not provided
    if output_dir is None:
        output_dir = os.path.dirname(input_path)

    return os.path.join(output_dir, filename)


# TODO: Re-think about if the exceptions are suitable
def report_file_size_reduction(input_path: str, output_path: str) -> None:
    """Report file size reduction.

    Parameters
    ----------
    input_path : str
        Path to input file.
    output_path : str
        Path to output file.
    """
    try:
        original_size = os.path.getsize(input_path)
        subset_size = os.path.getsize(output_path)
        reduction = (1 - subset_size / original_size) * 100
        print(f"File size reduction: {reduction:.1f}%")
    except (OSError, ZeroDivisionError) as e:
        print(f"Could not compute file size reduction: {e}")
