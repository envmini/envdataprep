"""Minimal demo: subsetting TROPOMI products as netCDF files."""

import glob
import os

import envdataprep as edp

# Directories
INPUT_DIR = "path/to/input/directory"
OUTPUT_DIR = "path/to/output/directory"

# Input files (using TROPOMI as an example)
input_files = glob.glob(os.path.join(INPUT_DIR, "S5P*.nc"))

# Explore all available variables
all_vars = edp.list_netcdf_vars(input_files[0])
print(*all_vars, sep="\n")

# Select the variables to keep
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

# Subset the netCDF files in parallel
edp.subset_netcdf(
    nc_input=input_files,
    output_dir=OUTPUT_DIR,
    keep_vars=selected_vars,
    workers=8,
)
