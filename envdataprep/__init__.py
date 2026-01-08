"""Top-level package for envdataprep."""

# Public APIs for users
from .core.netcdf import (
    list_netcdf_variables,
    netcdf_variable_to_xarray_dataarray,
    extract_netcdf_as_xarray_dataset,
    rename_xarray_dataset_variables,
    write_netcdf,
    subset_netcdf,
    compare_netcdf_variables,
    validate_netcdf_subset,
)

from .core.parallel import process_files_parallel

from .utils.io import handle_file_errors


# Version Number
__version__ = "0.1.0"


# Automatically define public APIs when all are imported
__all__ = [
    name for name in globals()
    if not name.startswith("_")
]
