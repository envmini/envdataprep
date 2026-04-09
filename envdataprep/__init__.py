"""Top-level package for envdataprep."""

from .core.netcdf import (
    list_netcdf_vars,
    convert_nc_var_to_dataarray,
    extract_netcdf_as_dataset,
    rename_dataset_vars,
    write_netcdf,
    check_netcdf,
    subset_netcdf,
)
from .sample_data import (
    make_sample_flat_nc_dataset,
    make_sample_grouped_nc_dataset,
    write_sample_flat_nc,
    write_sample_grouped_nc,
)


__version__ = "0.1.2"


__all__ = [
    name for name in globals()
    if not name.startswith("_")
]
