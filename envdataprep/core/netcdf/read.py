"""Reading and inspection functions for netCDF files."""

import warnings

import netCDF4 as nc
import xarray as xr

from ...utils.decorators import handle_file_errors, enable_parallel


def _collect_netcdf_var_paths(
    group: nc.Dataset, prefix: str = ""
) -> list[str]:
    """Recursively collect all variable paths from a netCDF group.

    Traverses a netCDF group hierarchy and returns all variable paths
    as a flat list using forward slashes as separators.

    Parameters
    ----------
    group : netCDF4.Dataset
        The netCDF group to traverse (can be root dataset or subgroup).
    prefix : str, default ""
        Current path prefix for building full variable paths.

    Returns
    -------
    list[str]
        All variable paths found in the group and its subgroups.
    """
    var_paths = []

    for var_name in group.variables:
        var_path = f"{prefix}/{var_name}" if prefix else var_name
        var_paths.append(var_path)

    for sub_name in group.groups:
        sub_path = f"{prefix}/{sub_name}" if prefix else sub_name
        var_paths.extend(
            _collect_netcdf_var_paths(
                group=group.groups[sub_name],
                prefix=sub_path,
            )
        )

    return var_paths


@handle_file_errors
def list_netcdf_vars(input_path: str) -> list[str]:
    """Get all available variable paths in a netCDF file.

    Parameters
    ----------
    input_path : str
        Path to netCDF file.

    Returns
    -------
    list[str]
        List of variable paths.
    """
    with nc.Dataset(input_path, "r") as ds:
        return _collect_netcdf_var_paths(group=ds)


@enable_parallel
def check_netcdf(
    input_path: str,
    deep: bool = False,
    require_vars: bool = True,
    workers: int | None = None,
    show_progress: bool = True,
) -> bool | list[tuple[str, bool | None, str | None]]:
    """Check whether a netCDF file can be opened and read.

    Useful for detecting corrupted or incomplete downloads
    before running expensive processing pipelines.

    Parameters
    ----------
    input_path : str or list[str] or tuple[str, ...]
        One path, or a sequence of paths to check.
    deep : bool, default False
        If False, only verify that the file opens. If True, walk the full
        group tree (via :func:`list_netcdf_vars`).
    require_vars : bool, default True
        Only applies when ``deep`` is True. If True, require at least one
        variable path; if False, a successfully opened file passes even
        with zero variables.
    workers : int, optional
        If greater than 1, run checks in parallel (multi-path only).
    show_progress : bool, default True
        Progress bar when ``workers > 1``.

    Returns
    -------
    bool
        Single path: whether the check passed.
    list[tuple[str, bool | None, str | None]]
        Multiple paths: one row per path, in order — ``(path, result, error)``.
        ``error`` is None on success; ``result`` is the boolean outcome.
    """
    try:
        if not deep:
            with nc.Dataset(input_path, "r"):
                pass
            return True
        nc_vars = list_netcdf_vars(input_path)
        if require_vars:
            return len(nc_vars) > 0
        return True
    except Exception as e:
        warnings.warn(
            f"Could not check netCDF file: {input_path} - {e}"
        )
        return False


def convert_nc_var_to_dataarray(
    nc_var: nc.Variable, var_path: str,
) -> xr.DataArray:
    """Convert a netCDF4 variable to an xarray DataArray.

    Preserves original fill values without converting them to NaNs,
    giving the user control over missing data representation.

    Parameters
    ----------
    nc_var : netCDF4.Variable
        The netCDF4 variable to convert.
    var_path : str
        Path/name for the variable.

    Returns
    -------
    xr.DataArray
        DataArray with original data values and attributes preserved.
    """
    attrs = {k: nc_var.getncattr(k) for k in nc_var.ncattrs()}

    # Disable automatic masking/scaling to preserve raw data exactly.
    # set_auto_maskandscale(False) disables both scale_factor/add_offset
    # and _FillValue-to-NaN conversion.
    nc_var.set_auto_maskandscale(False)

    return xr.DataArray(
        data=nc_var[:],
        dims=nc_var.dimensions,
        attrs=attrs,
        name=var_path,
    )


@handle_file_errors
def extract_netcdf_as_dataset(
    input_path: str, var_paths: list[str]
) -> xr.Dataset:
    """Extract variables using netCDF4, return as flat xarray Dataset.

    Parameters
    ----------
    input_path : str
        Path to netCDF file.
    var_paths : list[str]
        List of variable paths to extract (e.g., ["PRODUCT/latitude"]).

    Returns
    -------
    xr.Dataset
        Dataset containing extracted variables.
    """
    data_vars = {}

    with nc.Dataset(input_path, "r") as root_ds:
        for var_path in var_paths:
            try:
                nc_var = root_ds[var_path]
                da = convert_nc_var_to_dataarray(
                    nc_var=nc_var,
                    var_path=var_path,
                )
                data_vars[da.name] = da
            except KeyError:
                warnings.warn(
                    f"Variable '{var_path}' not found in {input_path}"
                )

        global_attrs = {k: root_ds.getncattr(k) for k in root_ds.ncattrs()}

    return xr.Dataset(data_vars, attrs=global_attrs)
