"""Top-level package for envdataprep."""

from .core.netcdf import (
    list_netcdf_vars,
    convert_nc_var_to_dataarray,
    extract_netcdf_as_dataset,
    rename_dataset_vars,
    write_netcdf,
    check_netcdf,
    subset_netcdf,
    compare_netcdf_vars,
    validate_netcdf_subset,
)

from .core.parallel import batch_process

from .utils.decorators import handle_file_errors, enable_parallel


__version__ = "0.1.2"


__all__ = [
    name for name in globals()
    if not name.startswith("_")
]
