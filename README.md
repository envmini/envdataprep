# EnvDataPrep: Optimized Environmental Data Preprocessing
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Development Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

## Why EnvDataPrep?
EnvDataPrep saves **Money**, **Time** and **Disk Storage** for those who deal with environmental datasets.

## Current Capacity
The first feature is the **Unified, Configuration-Driven Extraction of NetCDF Files**. Below is an example usage that reduces a TROPOMI satellite product file size by ~90%:
```python
"""Example of extracting a subset of variables from a netCDF file."""

from pathlib import Path
import envdataprep as edp

# Set up input and output directories
ROOT = Path("E:/Samples/Satellites")
input_dir = ROOT / "Input"
output_dir = ROOT / "Output"

# Select an example file
file_name = "S5P_RPRO_L2__NO2____20190101T233659_20190102T011828_06322_03_020400_20221106T093236.nc"
file_path = input_dir / file_name

# List all variables in the file
variables = edp.list_netcdf_variables(file_path)
print(*variables, sep='\n')

# List the variables to extract
variable_paths = [
    "PRODUCT/latitude",
    "PRODUCT/longitude",
    "PRODUCT/nitrogendioxide_tropospheric_column",
    "PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_fraction_crb",
]

# Extract and save out
# By default, the output file preserves the original group structure
edp.extract_and_write_netcdf(
    file_path,
    output_dir,
    variable_paths,
    output_name="example_extracted_data.nc",
    compression='zlib',
    compression_level=9,
)
```

## Installation

### Prerequisites
- **[Mamba](https://mamba.readthedocs.io/) (recommended) or [Conda](https://docs.conda.io/en/latest/)**
  - Preferred for installing scientific Python dependencies (netCDF4, xarray, numpy)
  - Handles complex dependency resolution more reliably than pip alone
- **Alternative**: pip installation should also work, but will be more complicated

### Quick Setup
```bash
# 1. Get the code
git clone https://github.com/envmini/envdataprep.git
cd envdataprep

# 2. Create environment (choose one)
# Option A: Using Mamba (faster, recommended)
mamba env create -f environment.yml
mamba activate envdataprep

# Option B: Using Conda
# conda env create -f environment.yml
# conda activate envdataprep

# 3. Install package in development mode
pip install -e . --no-deps

# 4. Verify installation
python -c "import envdataprep; print('Installation successful!')"
```
**Why development mode for now?**
- Package not yet published to PyPI/conda-forge
- Allows you to get latest features and contribute feedback
- Easy to update with `git pull`

**Note**: We use `pip install -e .` even in conda/mamba environments, but this `pip` command uses the pip from your active conda environment, not system pip, You can verify this with:
```bash
which pip  # Should show: .../miniforge3/envs/envdataprep/bin/pip
```

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

[⬆ Back to top](#envdataprep-extensible-environmental-data-preprocessing-framework)