"""Top-level package for envdataprep."""

from .core import io
from .core import process_files_parallel

# High-level convenience imports for most common functions
from .core.io import (
    extract_and_write_netcdf,
    list_netcdf_variables,
    extract_as_xr_dataset,
    write_netcdf,
)

__version__ = "0.1.0"

__all__ = [
    "io",
    "extract_and_write_netcdf",
    "list_netcdf_variables",
    "extract_as_xr_dataset",
    "write_netcdf",
    "process_files_parallel",
]
