"""Dummy data for tests, examples, and learning."""


from .netcdf import (
    make_dummy_flat_nc_dataset,
    make_dummy_grouped_nc_dataset,
    write_dummy_flat_nc,
    write_dummy_grouped_nc,
)

__all__ = [
    "make_dummy_flat_nc_dataset",
    "make_dummy_grouped_nc_dataset",
    "write_dummy_flat_nc",
    "write_dummy_grouped_nc",
]
