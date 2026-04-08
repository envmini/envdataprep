"""NetCDF processing functions.

Supports reading, writing, subsetting, comparing, and validating
netCDF data with optional compression and group-hierarchy handling.
"""

from ._helpers import DEFAULT_NETCDF_COMPLEVEL
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
    compare_netcdf_vars,
    validate_netcdf_subset,
    subset_netcdf,
)
from .spatial import (
    clip_netcdf_to_shape,
    clip_netcdf_to_bbox,
    sample_netcdf_at_points,
)
from .reshape import (
    split_netcdf_by_dim,
    merge_netcdf_by_dim,
)
