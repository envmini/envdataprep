"""Reading and inspection functions for netCDF files."""

import warnings

import netCDF4 as nc
import xarray as xr

from ...utils.decorators import handle_file_errors


def _collect_netcdf_var_paths(
    group: nc.Dataset, prefix: str = ""
) -> list[str]:
    """Recursively collect all variable paths from a netCDF group.

    Traverses a netCDF group hierarchy and returns all variable paths
    as a flat list using forward slashes as separators.

    Parameters
    ----------
    group : nc.Dataset
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
def list_netcdf_vars(nc_input: str) -> list[str]:
    """Get all available variable paths in a netCDF file.

    Parameters
    ----------
    nc_input : str
        Path to netCDF file.

    Returns
    -------
    list[str]
        List of variable paths.
    """
    with nc.Dataset(nc_input, "r") as ds:
        return _collect_netcdf_var_paths(group=ds)


def _check_netcdf_single(nc_input: str) -> str | None:
    """Check one netCDF file; return the path if it is faulty, else None.

    A file is treated as faulty if variables cannot be listed (after
    :func:`list_netcdf_vars` error handling) or the variable list is empty.

    Parameters
    ----------
    nc_input : str
        Path to netCDF file.

    Returns
    -------
    str or None
        ``nc_input`` if the file is faulty; ``None`` if it appears readable
        with at least one variable.
    """
    try:
        nc_vars = list_netcdf_vars(nc_input)
        if not nc_vars:
            warnings.warn(f"No variables found in {nc_input}")
            return nc_input
    except Exception as e:
        warnings.warn(f"Failed to read variables from {nc_input}: {e}")
        return nc_input
    return None


def check_netcdf(nc_input: str | list[str]) -> str | None | list[str]:
    """Check netCDF file(s) and return faulty paths."""

    # single file
    if isinstance(nc_input, str):
        return _check_netcdf_single(nc_input)

    # list of files (only use sequential processing for now)
    if isinstance(nc_input, list):
        all_checked = [_check_netcdf_single(f) for f in nc_input]
        faulty_files = [v for v in all_checked if v is not None]
        return faulty_files


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
