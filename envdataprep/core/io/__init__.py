"""Input/output functionality for processing environmental data."""

from .netcdf import (
    list_netcdf_variables,
    nc_var_to_xr_da,
    extract_as_xr_dataset,
    rename_xr_dataset_variables,
    write_netcdf,
    extract_and_write_netcdf,
)


__all__ = [
    "list_netcdf_variables",
    "nc_var_to_xr_da",
    "extract_as_xr_dataset",
    "rename_xr_dataset_variables",
    "write_netcdf",
    "extract_and_write_netcdf",
]
