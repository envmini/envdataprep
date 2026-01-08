"""
I/O functions and classes for netCDF files.

This module leverages two widely used Python libraries for working with
netCDF data: netCDF4 and xarray. The design combines high-performance,
low-level file access with a simple and expressive interface.

The netCDF4 library is used primarily for reading netCDF files, as it
provides direct access to nested group hierarchies and fine-grained
control over variables and metadata.

Xarray is used for writing netCDF files, benefiting from its concise
syntax and tight integration with labeled, multi-dimensional data
structures.

Utility functions are provided to convert between netCDF4 and xarray
objects where appropriate.
"""


import os
from collections import defaultdict
from typing import Dict, List, Optional

import netCDF4 as nc
import numpy as np
import xarray as xr

from ..utils.io import (
    handle_file_errors,
    generate_subset_output_path,
)


# TODO: Maybe re-think about the naming conventions
# TODO: Re-think about the compare function and validate function

def _collect_netcdf_variable_paths(
    group: nc.Dataset, path_prefix: str = ""
) -> List[str]:
    """Recursively collect all variable paths from a netCDF group.

    This function traverses a netCDF group and its subgroups to collect
    all variable paths in a flat list. Paths use forward slashes as separators.

    Parameters
    ----------
    group : netCDF4.Dataset
        The netCDF group to traverse (can be root dataset or subgroup)
    path_prefix : str, default ""
        Current path prefix for building full variable paths.

    Returns
    -------
    List[str]
        List of all variable paths found in the group and its subgroups.
    """
    variable_paths = []

    # Get variables from the current group
    for variable_name in group.variables:
        full_path = f"{path_prefix}/{variable_name}" if path_prefix else variable_name
        variable_paths.append(full_path)

    # Recursively get variables from subgroups
    # The path_prefix is essential for hierarchical netCDF files with groups
    for subgroup_name in group.groups:
        subgroup_path = (
            f"{path_prefix}/{subgroup_name}" if path_prefix else subgroup_name
        )
        subgroup_variables = _collect_netcdf_variable_paths(
            group=group.groups[subgroup_name],
            path_prefix=subgroup_path,
        )
        variable_paths.extend(subgroup_variables)

    return variable_paths


@handle_file_errors(file_format="netCDF")
def list_netcdf_variables(file_path: str) -> List[str]:
    """Get all available variable paths in a netCDF file.

    Parameters
    ----------
    file_path : str
        Path to netCDF file.

    Returns
    -------
    List[str]
        List of variable paths.
    """
    with nc.Dataset(file_path, "r") as ds:
        return _collect_netcdf_variable_paths(group=ds)


def netcdf_variable_to_xarray_dataarray(
    nc_variable: nc.Variable, variable_path: str
) -> xr.DataArray:
    """Convert a netCDF4 variable to an xarray DataArray.

    Parameters
    ----------
    nc_variable : netCDF4.Variable
        The netCDF variable to convert.
    variable_path : str
        Full path of the variable in the netCDF file.

    Returns
    -------
    xr.DataArray
        The converted DataArray with data, dimensions, and attributes.
    """
    attrs = {k: nc_variable.getncattr(k) for k in nc_variable.ncattrs()}

    # Disable auto-scaling to get raw values
    # This is crucial for data fields with scale factors
    # Such as the qa_value in TROPOMI products
    # TODO: Further test this with other data products
    # or add one more function to let user doublecheck the
    # correctness of the data, a function to compare data fields
    # in two netcdf files
    nc_variable.set_auto_scale(False)

    return xr.DataArray(
        data=nc_variable[:],
        dims=nc_variable.dimensions,
        attrs=attrs,
        name=variable_path,
    )


@handle_file_errors(file_format="netCDF")
def extract_netcdf_as_xarray_dataset(
    file_path: str, variable_paths: List[str]
) -> xr.Dataset:
    """Extract variables using netCDF4, return as flat xarray Dataset.

    Parameters
    ----------
    file_path : str
        Path to netCDF file.
    variable_paths : List[str]
        List of variable paths to extract (e.g., ["PRODUCT/latitude",]).
    rename_mapping : Dict[str, str], optional
        Mapping to rename variables {original_path: new_name}.

    Returns
    -------
    xr.Dataset
        Dataset containing extracted variables.
    """
    data_variables = {}

    with nc.Dataset(file_path, "r") as root_ds:
        for variable_path in variable_paths:
            try:
                nc_variable = root_ds[variable_path]
                data_array = netcdf_variable_to_xarray_dataarray(
                    nc_variable=nc_variable,
                    variable_path=variable_path,
                )
                data_variables[data_array.name] = data_array
            except KeyError as e:
                print(f"Warning: Variable '{variable_path}' not found: {e}")

        # Get global attributes
        global_attrs = {k: root_ds.getncattr(k) for k in root_ds.ncattrs()}

    xarray_dataset = xr.Dataset(data_variables, attrs=global_attrs)
    return xarray_dataset


def rename_xarray_dataset_variables(
    xarray_dataset: xr.Dataset, variable_renames: Dict[str, str]
) -> xr.Dataset:
    """Rename variables in an xarray dataset.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset to rename variables in.
    variable_renames : Dict[str, str]
        Mapping of {old_name: new_name} for renaming variables.

    Examples
    --------
    >>> renames = {"PRODUCT/latitude": "lat", "PRODUCT/longitude": "lon"}
    >>> renamed_ds = rename_xarray_dataset_variables(dataset, renames)
    """
    for old_name, new_name in variable_renames.items():
        xarray_dataset = xarray_dataset.rename({old_name: new_name})
    return xarray_dataset


def create_group_mapping(variable_names: List[str]) -> Dict[str, List[str]]:
    """Create mapping of group paths to variable names for hierarchical writing.

    Parameters
    ----------
    variable_names : List[str]
        Variable names (already renamed if needed). Can contain "/" for grouping.

    Returns
    -------
    Dict[str, List[str]]
        Mapping of {group_path: [variable_names, ...]}.
        Empty string key represents root group.
    """
    groups = defaultdict(list)

    for variable_name in variable_names:
        if "/" in variable_name:
            group_path, _ = variable_name.rsplit("/", 1)
        else:
            group_path = ""  # Root group

        groups[group_path].append(variable_name)

    return dict(groups)


def _create_encoding(
    xarray_dataset: xr.Dataset,
    compression_method: Optional[str] = "gzip",
    compression_level: int = 4,
    shuffle: bool = True,
    fletcher32: bool = True,
) -> Dict:
    """Create encoding dictionary for netCDF output.

    Encoding controls how xarray writes data to the netCDF file, including:
        - Compression algorithms to reduce file size
        - Data integrity checks (checksums)
        - Data layout optimizations (shuffle filter)

    Parameters
    ----------
    xarray_dataset : xr.Dataset
        Dataset containing variables to encode.
    compression : str, optional
        Compression algorithm ('zlib', 'lzf', 'gzip', 'szip').
        None means no compression.
    compression_level : int
        Compression strength (0-9). Higher = smaller files, slower write.
    shuffle : bool
        Reorder bytes before compression for better compression ratios.
        Especially effective for scientific data with similar values.
    fletcher32 : bool
        Add checksum for data integrity verification.

    Returns
    -------
    Dict
        Encoding dictionary mapping {variable_name: encoding_settings}.
        Example: {"temperature": {"zlib": True, "complevel": 4, "shuffle": True}}

    Examples
    --------
    >>> encoding = _create_encoding(dataset, "zlib", 4, True, False)
    >>> # Results in ~70% smaller files for typical environmental data
    """
    if compression_method is None:
        return {}  # No encoding = uncompressed output

    # Define how each compression algorithm maps to xarray/netCDF parameters
    compression_configs = {
        "zlib": lambda level: {
            "zlib": True, "complevel": level  # Most common, good balance
        },
        "lzf": lambda level: {
            "compression_method": "lzf",  # Fastest compression
        },
        "gzip": lambda level: {
            "compression_method": "gzip",  # Standard gzip
            "compression_opts": level,
        },
        "szip": lambda level: {
            "compression_method": "szip",  # NASA/HDF5 standard
        },
    }

    if compression_method not in compression_configs:
        valid_options = list(compression_configs.keys())
        raise ValueError(
            f"Unsupported compression: {compression_method}."
            f"Valid options: {valid_options}"
        )

    encoding = {}
    # Get the compression settings for the chosen algorithm
    compression_config = (
        compression_configs[compression_method](compression_level)
    )

    # Apply the same encoding settings to every variable in the dataset
    for variable_name in xarray_dataset.data_vars:
        # Start with compression settings (zlib, gzip, etc.)
        variable_encoding = compression_config.copy()

        # Add data optimization settings
        variable_encoding.update({
            "shuffle": shuffle,  # Reorder bytes for better compression
            "fletcher32": fletcher32  # Add checksum for data integrity
        })

        # Map variable name to its encoding settings
        # This tells xarray: "when writing 'temperature', use these settings"
        encoding[str(variable_name)] = variable_encoding

    return encoding


def _clean_group_variable_names(
    dataset: xr.Dataset, group_path: str
) -> xr.Dataset:
    """Remove group prefix from variable names for netCDF group writing.

    Transforms "PRODUCT/latitude" → "latitude" when writing to PRODUCT group.

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

    renamed_vars = {}
    prefix = f"{group_path}/"

    for name, var in dataset.data_vars.items():
        if name.startswith(prefix):
            clean_name = name[len(prefix):]
        else:
            clean_name = name
        renamed_vars[clean_name] = var

    return xr.Dataset(
        renamed_vars,
        coords=dataset.coords,
        attrs=dataset.attrs,
    )


def write_netcdf(
    xarray_dataset: xr.Dataset,
    output_path: str,
    compression_method: Optional[str] = "zlib",
    compression_level: int = 9,
    shuffle: bool = True,
    fletcher32: bool = False,
    **kwargs,
) -> str:
    """Write xarray Dataset to netCDF file with compression and group structure.

    Parameters
    ----------
    xarray_dataset : xr.Dataset
        Dataset to write. Variable names with "/" will create groups.
    output_path : str
        Full path for output file (including filename).
    compression_method : str, optional, default 'zlib'
        Compression algorithm ('zlib', 'lzf', 'gzip', 'szip', None).
    compression_level : int, default 9
        Compression level (0-9).
    shuffle : bool, default True
        Enable shuffle filter for better compression.
    fletcher32 : bool, default False
        Enable Fletcher32 checksum for error detection.
    **kwargs
        Additional arguments passed to Dataset.to_netcdf().

    Returns
    -------
    str
        Path to the created output file.
    """
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Create group mapping from variable names
    variable_names = list(xarray_dataset.data_vars.keys())
    group_mapping = create_group_mapping(variable_names)

    # Start writing
    first_write = True

    for group_path, variable_names in group_mapping.items():
        # Get subset of variables for this group
        group_variables = {
            name: xarray_dataset[name]
            for name in variable_names
            if name in xarray_dataset
        }

        if not group_variables:
            continue

        sub_dataset = xr.Dataset(
            group_variables,
            coords=xarray_dataset.coords,
            attrs=xarray_dataset.attrs,
        )

        # Clean variable names for this group (remove group prefix)
        sub_dataset = _clean_group_variable_names(sub_dataset, group_path)

        encoding = _create_encoding(
            xarray_dataset=sub_dataset,
            compression_method=compression_method,
            compression_level=compression_level,
            shuffle=shuffle,
            fletcher32=fletcher32,
        )

        sub_dataset.to_netcdf(
            output_path,
            mode="w" if first_write else "a",
            group=group_path if group_path else None,
            encoding=encoding,
            **kwargs,
        )

        # After writing out the first sub_dataset
        # The following writing should use Append mode
        first_write = False

    return output_path


def subset_netcdf(
    input_path: str,
    output_dir: str,
    variable_paths: List[str],
    output_name: Optional[str] = None,
    variable_renames: Optional[Dict[str, str]] = None,
    compression_method: Optional[str] = "zlib",
    compression_level: int = 4,
    shuffle: bool = True,
    fletcher32: bool = False,
    **kwargs,
) -> str:
    """Extract variables from netCDF and write to new file in one step.

    Parameters
    ----------
    input_path : str
        Path to input netCDF file.
    output_dir : str
        Output directory path.
    variable_paths : List[str]
        Variables to extract (e.g., ["PRODUCT/latitude", "PRODUCT/longitude"]).
    output_name : str, optional
        Output filename. If None, generates from input filename with _SUB suffix.
    variable_renames : Dict[str, str], optional
        Rename variables {old_name: new_name}.
    compression : str, optional, default 'zlib'
        Compression algorithm ('zlib', 'lzf', 'gzip', 'szip', None).
    compression_level : int, default 4
        Compression level (0-9).
    shuffle : bool, default True
        Enable shuffle filter for better compression.
    fletcher32 : bool, default False
        Enable Fletcher32 checksum for error detection.
    **kwargs
        Additional arguments passed to Dataset.to_netcdf().

    Returns
    -------
    str
        Path to created output file.
    """
    # Extract variables from netCDF
    xarray_dataset = extract_netcdf_as_xarray_dataset(
        file_path=input_path,
        variable_paths=variable_paths,
    )

    # Rename variables if needed
    if variable_renames:
        xarray_dataset = rename_xarray_dataset_variables(
            xarray_dataset=xarray_dataset,
            variable_renames=variable_renames,
        )

    # Generate output path
    output_path = generate_subset_output_path(
        input_path=input_path,
        output_dir=output_dir,
        custom_name=output_name
    )

    # Write to netCDF file
    return write_netcdf(
        xarray_dataset=xarray_dataset,
        output_path=output_path,
        compression_method=compression_method,
        compression_level=compression_level,
        shuffle=shuffle,
        fletcher32=fletcher32,
        **kwargs,
    )


# FIXME: This may not handle the NaNs well
def compare_netcdf_variables(
    file_a_path: str,
    file_b_path: str,
    variable_paths: Optional[List[str]] = None,
    exact_match: bool = True,
    tolerance: float = 1e-10,
    compare_attributes: bool = True
) -> Dict[str, Dict[str, bool]]:
    """Compare shared variables between two netCDF files.
    
    Parameters
    ----------
    file_a_path : str
        Path to first netCDF file.
    file_b_path : str
        Path to second netCDF file.
    variable_paths : List[str], optional
        Specific variables to compare. If None, compares all shared variables.
    exact_match : bool, default True
        If True, requires exact binary equality (no tolerance).
        If False, uses numerical tolerance for floating point comparisons.
    tolerance : float, default 1e-10
        Numerical tolerance (only used if exact_match=False).
    compare_attributes : bool, default True
        Whether to compare variable attributes.
        
    Returns
    -------
    Dict[str, Dict[str, bool]]
        Results for each variable: 
        {
            "PRODUCT/latitude": {"data_match": True, "attributes_match": True},
            "PRODUCT/longitude": {"data_match": False, "attributes_match": True}
        }
        
    Examples
    --------
    >>> # Compare subset against original (exact match)
    >>> results = compare_netcdf_variables("original.nc", "subset.nc")
    >>> 
    >>> # Compare two different files with tolerance
    >>> results = compare_netcdf_variables(
    ...     "file1.nc", "file2.nc", exact_match=False, tolerance=1e-6
    ... )
    """
    results = {}

    with nc.Dataset(file_a_path, "r") as ds_a, nc.Dataset(file_b_path, "r") as ds_b:

        # Determine which variables to compare
        if variable_paths is None:
            # If the variables are not specified
            # Find all shared variables
            variables_from_a = set(_collect_netcdf_variable_paths(ds_a))
            variables_from_b = set(_collect_netcdf_variable_paths(ds_b))
            variable_paths = list(variables_from_a.intersection(variables_from_b))

        for variable_path in variable_paths:
            variable_results = {"data_match": False, "attributes_match": False}

            try:
                variable_a = ds_a[variable_path]
                variable_b = ds_b[variable_path]

                # Compare data - exact or with tolerance
                if exact_match:
                    variable_results["data_match"] = np.array_equal(variable_a[:], variable_b[:])
                else:
                    try:
                        variable_results["data_match"] = np.allclose(
                            variable_a[:],
                            variable_b[:],
                            rtol=tolerance,
                            atol=tolerance,
                            equal_nan=True,
                        )
                    except (ValueError, TypeError):
                        # Fallback to exact for non-numeric data
                        variable_results["data_match"] = np.array_equal(
                            variable_a[:], variable_b[:], equal_nan=True
                        )

                # Compare attributes if requested
                if compare_attributes:
                    attrs_a = {k: variable_a.getncattr(k) for k in variable_a.ncattrs()}
                    attrs_b = {k: variable_b.getncattr(k) for k in variable_b.ncattrs()}
                    variable_results["attributes_match"] = attrs_a == attrs_b
                else:
                    variable_results["attributes_match"] = True

            except KeyError as e:
                print(f"Warning: Variable '{variable_path}' not found in one of the files: {e}")
                continue

            results[variable_path] = variable_results

    return results


def validate_netcdf_subset(
    original_path: str,
    subset_path: str,
    variable_paths: Optional[List[str]] = None,
    exact_match: bool = True,
    tolerance: float = 1e-10
) -> bool:
    """Validate that netCDF subset preserves data values exactly.
    
    This is a convenience wrapper around compare_netcdf_variables for subset validation.
    
    Parameters
    ----------
    original_path : str
        Path to original netCDF file.
    subset_path : str
        Path to subset netCDF file.
    variable_paths : List[str]
        Variables that were extracted in the subset.
    exact_match : bool, default True
        If True, requires exact binary equality.
    tolerance : float, default 1e-10
        Numerical tolerance (only used if exact_match=False).
        
    Returns
    -------
    bool
        True if all variables match exactly, False otherwise.
        
    Raises
    ------
    ValueError
        If any variables don't match, with details about differences.
        
    Examples
    --------
    >>> # Validate exact subset
    >>> validate_netcdf_subset(
    ...     "original.nc", "subset.nc", 
    ...     ["PRODUCT/latitude", "PRODUCT/longitude"]
    ... )
    True
    """
    results = compare_netcdf_variables(
        file_a_path=original_path,
        file_b_path=subset_path,
        variable_paths=variable_paths,
        exact_match=exact_match,
        tolerance=tolerance,
    )

    failed_vars = []
    for variable_path, variable_results in results.items():
        if not all(variable_results.values()):
            failed_vars.append(variable_path)

    if failed_vars:
        raise ValueError(f"Validation failed for variables: {failed_vars}")

    return True
