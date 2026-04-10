# EnvDataPrep: High-performance Environmental Data Pre-processing

[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Development Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

## Why EnvDataPrep?

**EnvDataPrep** aims to help environmental scientists overcome common challenges in handling environmental datasets, incluidng:
- Insufficient disk space for massive datasets
- Complex conversions between different file formats
- Time-consuming geospatial operations
- And more ...

EnvDataPrep is designed for *high performance* and *syntax simplicity*. By leveraging vectorized operations, parallelism, and industry-standard libraries like **`NumPy`**, **`Xarray`**, and **`netCDF4`**, it streamlines heavy and complex data preparation tasks into efficient and easy-to-use APIs.

## Core Capacities

Currently, the main functionality is **subsetting netCDF files**. Typical satellite products (e.g., TROPOMI NO<sub>2</sub>) and model simulations (e.g., WRF outputs) can shrink by a large fraction (e.g., 90%) when you keep only the data fields you need.

### Example of subsetting netCDF files

```python
import glob
import os

import envdataprep as edp

# Directories
INPUT_DIR = "path/to/input/directory"
OUTPUT_DIR = "path/to/output/directory"

# Input files (using TROPOMI NO2 satellite products as an example)
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
```

## Installation

**Requirements:** Python **3.12+**.

Install from [PyPI](https://pypi.org/project/envdataprep/):

```bash
pip install envdataprep
```


## ⚠️ Disclaimer
Due to the massive scale and inherent diversity of environmental data, some edge cases may remain unexplored. For critical research or production workflows, it is strongly recommended to manually validate processed outputs. 

If you encounter any discrepancies or unexpected behavior, please [open an issue](https://github.com/envmini/envdataprep/issues).

## License

This project is licensed under the [MIT License](LICENSE).

[⬆ Back to top](#envdataprep-high-performance-environmental-data-pre-processing)
