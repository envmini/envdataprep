# # envdataprep/core/conversions.py
# """Cross-format data conversions."""

# from typing import Optional, Dict, Any
# from pathlib import Path

# # Import from format-specific modules
# from .netcdf import extract_netcdf_as_xarray_dataset
# from .tiff import write_geotiff  # Future
# from .grib2 import write_grib2   # Future


# def netcdf_to_tiff(
#     netcdf_path: str,
#     output_path: str,
#     variable_name: str,
#     crs: Optional[str] = None,
#     **kwargs
# ) -> str:
#     """Convert NetCDF variable to GeoTIFF.

#     Parameters
#     ----------
#     netcdf_path : str
#         Path to input NetCDF file.
#     output_path : str
#         Path for output TIFF file.
#     variable_name : str
#         Variable to convert (must be 2D with lat/lon).
#     crs : str, optional
#         Coordinate reference system (e.g., 'EPSG:4326').
#     """
#     # Extract data using netcdf module
#     dataset = extract_netcdf_as_xarray_dataset(netcdf_path, [variable_name])
    
#     # Convert to TIFF using tiff module
#     return write_geotiff(dataset[variable_name], output_path, crs=crs, **kwargs)


# def netcdf_to_grib2(
#     netcdf_path: str,
#     output_path: str,
#     variable_mapping: Dict[str, str],
#     **kwargs
# ) -> str:
#     """Convert NetCDF to GRIB2 format."""
#     # Implementation using both netcdf and grib2 modules
#     pass


# def tiff_to_netcdf(
#     tiff_path: str,
#     output_path: str,
#     variable_name: str = "data",
#     **kwargs
# ) -> str:
#     """Convert GeoTIFF to NetCDF format."""
#     # Implementation using both tiff and netcdf modules
#     pass


# # Matrix of all possible conversions
# SUPPORTED_CONVERSIONS = {
#     ("netcdf", "tiff"): netcdf_to_tiff,
#     ("netcdf", "grib2"): netcdf_to_grib2,
#     ("tiff", "netcdf"): tiff_to_netcdf,
#     ("tiff", "grib2"): None,  # Future
#     ("grib2", "netcdf"): None,  # Future
#     ("grib2", "tiff"): None,   # Future
# }


# def _detect_format(file_path: str) -> str:
#     """Detect file format from extension.
    
#     Parameters
#     ----------
#     file_path : str
#         Path to file.
        
#     Returns
#     -------
#     str
#         Detected format ('netcdf', 'tiff', 'grib2').
        
#     Raises
#     ------
#     ValueError
#         If format cannot be detected from extension.
#     """
#     from pathlib import Path
    
#     suffix = Path(file_path).suffix.lower()
    
#     format_mapping = {
#         '.nc': 'netcdf',
#         '.nc4': 'netcdf', 
#         '.netcdf': 'netcdf',
#         '.tif': 'tiff',
#         '.tiff': 'tiff',
#         '.gtiff': 'tiff',
#         '.grib': 'grib2',
#         '.grib2': 'grib2',
#         '.grb': 'grib2',
#         '.grb2': 'grib2',
#     }
    
#     if suffix not in format_mapping:
#         raise ValueError(
#             f"Cannot detect format from extension '{suffix}'. "
#             f"Supported extensions: {list(format_mapping.keys())}"
#         )
    
#     return format_mapping[suffix]


# def convert_format(
#     input_path: str,
#     output_path: str,
#     **kwargs
# ) -> str:
#     """Generic format conversion function with auto-detection.
    
#     Automatically detects source and target formats from file extensions.
    
#     Parameters
#     ----------
#     input_path : str
#         Path to input file (format detected from extension).
#     output_path : str
#         Path for output file (format detected from extension).
#     **kwargs
#         Additional arguments passed to specific conversion function.
        
#     Returns
#     -------
#     str
#         Path to created output file.
        
#     Examples
#     --------
#     >>> # NetCDF to GeoTIFF - formats auto-detected
#     >>> convert_format("data.nc", "output.tif", variable_name="temperature")
#     >>> 
#     >>> # GRIB2 to NetCDF - formats auto-detected  
#     >>> convert_format("weather.grib2", "climate.nc")
#     """
#     # Auto-detect formats
#     source_format = _detect_format(input_path)
#     target_format = _detect_format(output_path)
    
#     conversion_key = (source_format, target_format)
    
#     if conversion_key not in SUPPORTED_CONVERSIONS:
#         raise ValueError(
#             f"Conversion from {source_format} to {target_format} not supported. "
#             f"Supported conversions: {list(SUPPORTED_CONVERSIONS.keys())}"
#         )
    
#     converter = SUPPORTED_CONVERSIONS[conversion_key]
#     if converter is None:
#         raise NotImplementedError(
#             f"Conversion from {source_format} to {target_format} not yet implemented"
#         )
    
#     return converter(input_path, output_path, **kwargs)


# # Optional: Keep explicit version for edge cases
# def convert_format_explicit(
#     input_path: str,
#     output_path: str,
#     source_format: str,
#     target_format: str,
#     **kwargs
# ) -> str:
#     """Format conversion with explicit format specification.
    
#     Use this when auto-detection fails or for non-standard extensions.
#     """
#     conversion_key = (source_format.lower(), target_format.lower())
    
#     if conversion_key not in SUPPORTED_CONVERSIONS:
#         raise ValueError(f"Conversion from {source_format} to {target_format} not supported")
    
#     converter = SUPPORTED_CONVERSIONS[conversion_key]
#     if converter is None:
#         raise NotImplementedError(f"Conversion from {source_format} to {target_format} not yet implemented")
    
#     return converter(input_path, output_path, **kwargs)
