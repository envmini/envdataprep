"""I/O functions and classes for netCDF files."""

import os
from collections import defaultdict
from typing import Dict, List, Optional

import xarray as xr
from netCDF4 import Dataset


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
    with Dataset(file_path, "r") as ds:
        return _collect_variable_paths(ds)


def _collect_variable_paths(
    group: Dataset, path_prefix: str = ""
) -> List[str]:
    """Recursively collect all variable paths from a netCDF group.

    This helper function traverses a netCDF group and its subgroups to collect
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
    for var_name in group.variables:
        full_path = f"{path_prefix}/{var_name}" if path_prefix else var_name
        variable_paths.append(full_path)

    # Recursively get variables from subgroups
    for subgroup_name in group.groups:
        subgroup_path = (
            f"{path_prefix}/{subgroup_name}" if path_prefix else subgroup_name
        )
        subgroup_vars = _collect_variable_paths(
            group.groups[subgroup_name], subgroup_path
        )
        variable_paths.extend(subgroup_vars)

    return variable_paths


def nc_var_to_xr_da(nc_var, var_path: str) -> xr.DataArray:
    """Convert a netCDF variable to an xarray DataArray.

    Parameters
    ----------
    nc_var : netCDF4.Variable
        The netCDF variable to convert.
    var_path : str
        Full path of the variable in the netCDF file.
    rename_mapping : dict, optional
        Mapping to rename variables during conversion.

    Returns
    -------
    xr.DataArray
        The converted DataArray with data, dimensions, and attributes.
    """
    attrs = {k: nc_var.getncattr(k) for k in nc_var.ncattrs()}

    # Disable auto-scaling to get raw values
    # This is crucial for data fields with scale factors
    # Such as the qa_value in TROPOMI products
    # TODO: Further test this with other data products
    # or add one more function to let user doublecheck the
    # correctness of the data, a function to compare data fields
    # in two netcdf files
    nc_var.set_auto_scale(False)

    return xr.DataArray(
        data=nc_var[:],
        dims=nc_var.dimensions,
        attrs=attrs,
        name=var_path,
    )


def extract_as_xr_dataset(
    file_path: str, variable_paths: List[str]
) -> xr.Dataset:
    """Extract variables using netCDF4, return as flat xarray Dataset.

    Parameters
    ----------
    file_path : str
        Path to netCDF file.
    variable_paths : List[str]
        List of variable paths to extract (e.g., ["PRODUCT/latitude", "PRODUCT/longitude"]).
    rename_mapping : Dict[str, str], optional
        Mapping to rename variables {original_path: new_name}.

    Returns
    -------
    xr.Dataset
        Dataset containing extracted variables.
    """
    data_arrays = {}

    with Dataset(file_path, "r") as root_ds:
        for var_path in variable_paths:
            try:
                nc_var = root_ds[var_path]
                data_array = nc_var_to_xr_da(nc_var, var_path)
                data_arrays[data_array.name] = data_array
            except KeyError as e:
                print(f"Warning: Variable '{var_path}' not found: {e}")

        # Get global attributes
        global_attrs = {k: root_ds.getncattr(k) for k in root_ds.ncattrs()}

    dataset = xr.Dataset(data_arrays, attrs=global_attrs)
    return dataset


def rename_xr_dataset_variables(
    dataset: xr.Dataset, variable_renames: Dict[str, str]
) -> xr.Dataset:
    """Rename variables in an xarray dataset"""
    for new_name, old_name in variable_renames.items():
        dataset = dataset.rename({old_name: new_name})
    return dataset


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

    for var_name in variable_names:
        if "/" in var_name:
            group_path, _ = var_name.rsplit("/", 1)
        else:
            group_path = ""  # Root group

        groups[group_path].append(var_name)

    return dict(groups)


class NetCDFWriter:
    """Write xarray datasets to netCDF with compression and group structure preservation.

    Parameters
    ----------
    compression : str, optional
        Compression algorithm ('zlib', 'lzf', 'gzip', 'szip').
    compression_level : int, default 4
        Compression level (0-9). Higher values = better compression.
    shuffle : bool, default True
        Enable shuffle filter for better compression.
    fletcher32 : bool, default False
        Enable Fletcher32 checksum for error detection.
    """

    _COMPRESSION_CONFIGS = {
        "zlib": lambda level: {"zlib": True, "complevel": level},
        "lzf": lambda level: {"compression": "lzf"},
        "gzip": lambda level: {
            "compression": "gzip",
            "compression_opts": level,
        },
        "szip": lambda level: {"compression": "szip"},
    }

    def __init__(
        self,
        compression: Optional[str] = None,
        compression_level: int = 4,
        shuffle: bool = True,
        fletcher32: bool = False,
    ):
        """Initialize NetCDFWriter."""
        self.compression = compression
        self.compression_level = compression_level
        self.shuffle = shuffle
        self.fletcher32 = fletcher32

    def write(self, dataset: xr.Dataset, output_path: str, **kwargs) -> str:
        """Write dataset preserving group structure based on variable names.

        Parameters
        ----------
        dataset : xr.Dataset
            Dataset to write. Variable names with "/" will create groups.
        output_path : str
            Path for output file.
        **kwargs
            Additional arguments passed to Dataset.to_netcdf().

        Returns
        -------
        str
            Path to the created output file.
        """
        # Create group mapping from variable names
        variable_names = list(dataset.data_vars.keys())
        group_mapping = create_group_mapping(variable_names)

        first_write = True

        for group_path, var_names in group_mapping.items():
            # Get subset of variables for this group
            group_vars = {
                name: dataset[name] for name in var_names if name in dataset
            }
            if not group_vars:
                continue

            sub_dataset = xr.Dataset(
                group_vars, coords=dataset.coords, attrs=dataset.attrs
            )

            # Clean variable names for this group (remove group prefix)
            sub_dataset = self._clean_group_variable_names(
                sub_dataset, group_path
            )

            encoding = self._create_encoding(sub_dataset)

            sub_dataset.to_netcdf(
                output_path,
                mode="w" if first_write else "a",
                group=group_path if group_path else None,
                encoding=encoding,
                **kwargs,
            )
            first_write = False

        return output_path

    def _clean_group_variable_names(
        self, dataset: xr.Dataset, group_path: str
    ) -> xr.Dataset:
        """Remove group prefix from variable names."""
        if not group_path:
            return dataset

        renamed_vars = {}
        prefix = f"{group_path}/"

        for name, var in dataset.data_vars.items():
            if name.startswith(prefix):
                clean_name = name[len(prefix) :]
            else:
                clean_name = name
            renamed_vars[clean_name] = var

        return xr.Dataset(
            renamed_vars, coords=dataset.coords, attrs=dataset.attrs
        )

    def _create_encoding(self, dataset: xr.Dataset) -> Dict:
        """Create encoding dictionary for netCDF output."""
        if self.compression is None:
            return {}

        if self.compression not in self._COMPRESSION_CONFIGS:
            valid_options = list(self._COMPRESSION_CONFIGS.keys())
            raise ValueError(
                f"Unsupported compression: {self.compression}. "
                f"Valid options: {valid_options}"
            )

        encoding = {}
        compression_config = self._COMPRESSION_CONFIGS[self.compression](
            self.compression_level
        )

        for var_name in dataset.data_vars:
            var_encoding = compression_config.copy()
            var_encoding.update(
                {"shuffle": self.shuffle, "fletcher32": self.fletcher32}
            )

            encoding[str(var_name)] = var_encoding

        return encoding


def generate_output_path(
    input_path: str, output_dir: str, custom_name: Optional[str] = None
) -> str:
    """Generate output file path with _SUB suffix in specified directory.

    Parameters
    ----------
    input_path : str
        Path to input file.
    output_dir : str
        Output directory path.
    custom_name : str, optional
        Custom filename to use instead of generating one.

    Returns
    -------
    str
        Full output file path.
    """
    if custom_name:
        filename = custom_name
    else:
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        extension = os.path.splitext(input_path)[1]
        filename = f"{base_name}_SUB{extension}"

    return os.path.join(output_dir, filename)


def write_netcdf(
    dataset: xr.Dataset,
    output_dir: str,
    output_name: Optional[str] = None,
    compression: Optional[str] = "zlib",
    compression_level: int = 9,
    shuffle: bool = True,
    fletcher32: bool = False,
    **kwargs,
) -> str:
    """Write xarray Dataset to netCDF file with compression.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset to write.
    output_dir : str
        Output directory path.
    output_name : str, optional
        Output filename. If None, uses 'dataset_SUB.nc'.
    compression : str, optional, default 'zlib'
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
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename if not provided
    if output_name is None:
        output_name = "dataset_SUB.nc"

    output_path = os.path.join(output_dir, output_name)

    writer = NetCDFWriter(
        compression=compression,
        compression_level=compression_level,
        shuffle=shuffle,
        fletcher32=fletcher32,
    )
    return writer.write(dataset, output_path, **kwargs)


def set_subset_output_name(
    file_path: str, custom_name: Optional[str] = None
) -> str:
    """Generate output filename with _SUB suffix.

    Parameters
    ----------
    file_path : str
        Path to input file.
    custom_name : str, optional
        Custom filename to use instead of generating one.

    Returns
    -------
    str
        Output filename with _SUB suffix or the custom name.
    """
    if custom_name:
        return custom_name

    base_name = os.path.splitext(os.path.basename(file_path))[0]
    extension = os.path.splitext(file_path)[1]
    return f"{base_name}_SUB{extension}"


def extract_and_write_netcdf(
    input_path: str,
    output_dir: str,
    variable_paths: List[str],
    output_name: Optional[str] = None,
    variable_renames: Optional[Dict[str, str]] = None,
    compression: Optional[str] = "zlib",
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
        Rename variables {new_name: old_name}.
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
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate output path
    output_path = generate_output_path(input_path, output_dir, output_name)

    # Extract
    dataset = extract_as_xr_dataset(input_path, variable_paths)

    # Rename if needed
    if variable_renames:
        dataset = rename_xr_dataset_variables(dataset, variable_renames)

    # Write
    write_netcdf(
        dataset,
        output_dir,
        os.path.basename(output_path),
        compression=compression,
        compression_level=compression_level,
        shuffle=shuffle,
        fletcher32=fletcher32,
        **kwargs,
    )
