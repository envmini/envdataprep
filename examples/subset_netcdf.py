"""Minimal demo: subsetting TROPOMI products as netCDF files."""

# Imports
import glob
import os

import envdataprep as edp

# User configuration
INPUT_DIR = "path/to/input/directory"
OUTPUT_DIR = "path/to/output/directory"

selected_vars = [
    "PRODUCT/latitude",
    "PRODUCT/longitude",
    "PRODUCT/time_utc",
    "PRODUCT/qa_value",
    "PRODUCT/nitrogendioxide_tropospheric_column",
    "PRODUCT/nitrogendioxide_tropospheric_column_precision",
    "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle",
    "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle",
    "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/latitude_bounds",
    "PRODUCT/SUPPORT_DATA/GEOLOCATIONS/longitude_bounds",
    "PRODUCT/SUPPORT_DATA/INPUT_DATA/surface_altitude",
    "PRODUCT/SUPPORT_DATA/INPUT_DATA/eastward_wind",
    "PRODUCT/SUPPORT_DATA/INPUT_DATA/northward_wind",
]

# The parallel subsetting process
input_files = glob.glob(os.path.join(INPUT_DIR, "S5P*.nc"))

edp.subset_netcdf(
    input_path=input_files,
    output_dir=OUTPUT_DIR,
    keep_vars=selected_vars,
    workers=8
)
