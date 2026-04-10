"""NetCDF processing functions.

Supports reading, writing, subsetting, comparing, and validating
netCDF data with optional compression and group-hierarchy handling.
"""

from .read import (
    list_netcdf_vars,
    check_netcdf,
    convert_nc_var_to_dataarray,
    extract_netcdf_as_dataset,
)
from .write import (
    rename_dataset_vars,
    write_netcdf,
)
from .subset import (
    subset_netcdf,
)

__all__ = [
    "list_netcdf_vars",
    "check_netcdf",
    "convert_nc_var_to_dataarray",
    "extract_netcdf_as_dataset",
    "rename_dataset_vars",
    "write_netcdf",
    "subset_netcdf",
]
