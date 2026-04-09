"""Writing and encoding functions for netCDF files."""

import os
from collections import defaultdict

import xarray as xr

from ...utils.constants import DEFAULT_NETCDF_COMPLEVEL


def rename_dataset_vars(
    dataset: xr.Dataset, var_renames: dict[str, str]
) -> xr.Dataset:
    """Rename variables in an xarray dataset.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset to rename variables in.
    var_renames : dict[str, str]
        Mapping of {old_name: new_name}.

    Returns
    -------
    xr.Dataset
        Dataset with renamed variables.
    """
    for old_name, new_name in var_renames.items():
        dataset = dataset.rename({old_name: new_name})
    return dataset


def _create_group_mapping(var_names: list[str]) -> dict[str, list[str]]:
    """Create mapping of group paths to variable names for hierarchical writing.

    Parameters
    ----------
    var_names : list[str]
        Variable names. Can contain "/" for grouping.

    Returns
    -------
    dict[str, list[str]]
        Mapping of {group_path: [var_names, ...]}.
        Empty string key represents root group.
    """
    groups = defaultdict(list)

    for var_name in var_names:
        if "/" in var_name:
            group_path, _ = var_name.rsplit("/", 1)
        else:
            group_path = ""
        groups[group_path].append(var_name)

    return dict(groups)


def _clean_group_var_names(
    dataset: xr.Dataset, group_path: str
) -> xr.Dataset:
    """Remove group prefix from variable names for netCDF group writing.

    Transforms "PRODUCT/latitude" -> "latitude" when writing to PRODUCT group.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset with potentially prefixed variable names.
    group_path : str
        Group path prefix to remove (e.g., "PRODUCT").

    Returns
    -------
    xr.Dataset
        Dataset with cleaned variable names.
    """
    if not group_path:
        return dataset

    prefix = f"{group_path}/"
    renamed = {}
    for name, var in dataset.data_vars.items():
        if str(name).startswith(prefix):
            clean = str(name)[len(prefix):]
        else:
            clean = name
        renamed[clean] = var

    return xr.Dataset(renamed, coords=dataset.coords, attrs=dataset.attrs)


def _create_encoding(
    dataset: xr.Dataset,
    compression: str | None = "zlib",
    complevel: int = DEFAULT_NETCDF_COMPLEVEL,
    shuffle: bool = True,
    fletcher32: bool = False,
) -> dict:
    """Create encoding dictionary for netCDF output.

    Only compresses numeric variables larger than 1KB to avoid
    encoding conflicts.
    """
    if compression is None:
        return {}

    # Currently only zlib is supported;
    # other algorithms (gzip, szip, lzf)
    # produced errors in testing.
    comp_configs = {
        "zlib": lambda level: {"zlib": True, "complevel": level},
    }

    if compression not in comp_configs:
        raise ValueError(
            f"Unsupported compression: {compression}. "
            f"Valid options: {list(comp_configs.keys())}"
        )

    encoding = {}
    config = comp_configs[compression](complevel)

    for var_name in dataset.data_vars:
        var = dataset[var_name]
        if var.dtype.kind in ("f", "i", "u") and var.nbytes > 1024:
            var_enc = config.copy()
            var_enc.update({"shuffle": shuffle, "fletcher32": fletcher32})
            encoding[str(var_name)] = var_enc

    return encoding


def write_netcdf(
    dataset: xr.Dataset,
    output_path: str,
    compression: str | None = "zlib",
    complevel: int = DEFAULT_NETCDF_COMPLEVEL,
    shuffle: bool = True,
    fletcher32: bool = False,
    **kwargs,
) -> None:
    """Write xarray Dataset to netCDF file with compression and group structure.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset to write. Variable names with "/" will create groups.
    output_path : str
        Full path for output file (including filename).
    compression : str or None, default 'zlib'
        Compression algorithm. Currently only 'zlib' is supported.
        Pass None to disable compression.
    complevel : int, default 4
        Compression level (0-9).
    shuffle : bool, default True
        Enable shuffle filter for better compression.
    fletcher32 : bool, default False
        Enable Fletcher32 checksum for error detection.
    **kwargs
        Additional arguments passed to Dataset.to_netcdf().
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    var_names = [str(k) for k in dataset.data_vars.keys()]
    group_map = _create_group_mapping(var_names)

    # Ensure root group exists to preserve global attributes
    if "" not in group_map:
        group_map[""] = []

    # Write root group first (empty string sorts first)
    sorted_groups = sorted(group_map.items(), key=lambda x: (x[0] != "", x[0]))
    first_write = True

    for group_path, group_var_names in sorted_groups:
        group_vars = {
            name: dataset[name]
            for name in group_var_names
            if name in dataset
        }
        group_attrs = dataset.attrs if group_path == "" else {}

        sub_ds = xr.Dataset(
            data_vars=group_vars,
            coords=dataset.coords,
            attrs=group_attrs,
        )

        sub_ds = _clean_group_var_names(sub_ds, group_path)

        encoding = _create_encoding(
            dataset=sub_ds,
            compression=compression,
            complevel=complevel,
            shuffle=shuffle,
            fletcher32=fletcher32,
        )

        sub_ds.to_netcdf(
            output_path,
            mode="w" if first_write else "a",
            group=group_path if group_path else None,
            encoding=encoding,
            **kwargs,
        )
        first_write = False
