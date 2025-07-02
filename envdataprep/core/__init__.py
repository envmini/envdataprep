"""Core functionality for processing environmental data."""

from .io import (
    list_netcdf_variables,
    extract_netcdf_variable,
    NetCDFProcessor,
)

from .datasets import (
    GEMSProcessor,
    TEMPOProcessor,
    TROPOMIProcessor,
)


__all__ = [
    "list_netcdf_variables",
    "extract_netcdf_variable",
    "NetCDFProcessor",
    "GEMSProcessor",
    "TEMPOProcessor",
    "TROPOMIProcessor",
]
