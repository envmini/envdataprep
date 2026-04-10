"""Spatial clipping and point sampling for netCDF files."""


def clip_netcdf_to_shape(
    input_path: str,
    shape_path: str,
    output_path: str,
    var_paths: list[str] | None = None,
    **kwargs,
) -> str:
    """Clip NetCDF data to geographic shape boundary.

    Extracts only data points that fall within the provided shape,
    significantly reducing file size for regional studies.

    Parameters
    ----------
    input_path : str
        Path to input NetCDF file.
    shape_path : str
        Path to shape file (GeoJSON, Shapefile, etc.).
    output_path : str
        Path for clipped output file.
    var_paths : list[str], optional
        Variables to include. If None, includes all variables.
    """
    raise NotImplementedError("clip_netcdf_to_shape is not implemented yet.")


def clip_netcdf_to_bbox(
    input_path: str,
    bbox: tuple[float, float, float, float],
    output_path: str,
    var_paths: list[str] | None = None,
    **kwargs,
) -> str:
    """Clip NetCDF data to bounding box.

    Simpler alternative to shape clipping for rectangular regions.

    Parameters
    ----------
    input_path : str
        Path to input NetCDF file.
    bbox : tuple[float, float, float, float]
        Bounding box as (min_lon, min_lat, max_lon, max_lat).
    output_path : str
        Path for clipped output file.
    var_paths : list[str], optional
        Variables to include. If None, includes all variables.
    """
    raise NotImplementedError("clip_netcdf_to_bbox is not implemented yet.")


def sample_netcdf_at_points(
    input_path: str,
    points: list[tuple[float, float]],
    output_path: str,
    var_paths: list[str] | None = None,
    **kwargs,
) -> str:
    """Extract NetCDF data at specific point locations.

    Useful for validation against ground stations or creating
    time series at specific locations.

    Parameters
    ----------
    input_path : str
        Path to input NetCDF file.
    points : list[tuple[float, float]]
        Coordinates as [(lon, lat), ...].
    output_path : str
        Path for output file.
    var_paths : list[str], optional
        Variables to include. If None, includes all variables.
    """
    raise NotImplementedError("sample_netcdf_at_points is not implemented yet.")
