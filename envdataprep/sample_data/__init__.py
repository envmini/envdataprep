"""Synthetic sample data for tests, examples, and learning.

Names use ``sample_`` so they are not confused with core APIs such as
:func:`~envdataprep.core.netcdf.subset_netcdf` or
:func:`~envdataprep.core.netcdf.write_netcdf`.

Format-specific builders live in submodules (e.g. :mod:`netcdf`). An ``nc``
infix marks netCDF-oriented xarray datasets; other formats can add parallel
names later (e.g. ``make_sample_flat_grib_...``).

Examples
--------
>>> from envdataprep.sample_data import write_sample_flat_nc
>>> write_sample_flat_nc("demo_flat.nc")
'demo_flat.nc'
"""

from .netcdf import (
    make_sample_flat_nc_dataset,
    make_sample_grouped_nc_dataset,
    write_sample_flat_nc,
    write_sample_grouped_nc,
)

__all__ = [
    "make_sample_flat_nc_dataset",
    "make_sample_grouped_nc_dataset",
    "write_sample_flat_nc",
    "write_sample_grouped_nc",
]
