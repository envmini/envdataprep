"""Input/output functionality for processing environmental data."""

from .netcdf import (
    list_netcdf_variables,
    extract_netcdf_variable,
    NetCDFProcessor,
)


__all__ = [
    "list_netcdf_variables",
    "extract_netcdf_variable",
    "NetCDFProcessor"
]
