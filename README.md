# EnvDataPrep: Extensible Environmental Data Preprocessing Framework

[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Development Status](https://img.shields.io/badge/status-alpha-orange.svg)]()


## Overview

**EnvDataPrep** is a high-performance, extensible Python framework for preprocessing environmental datasets. Built on modern software engineering principles, it transforms raw environmental data from various sources (satellites, reanalysis, models) into research-ready formats with automatic performance optimization and seamless scalability from laptop to HPC.

## Table of Contents

- [Why EnvDataPrep](#why-envdataprep)
- [Current Capacity](#current-capacity)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture & Design](#architecture--design)
- [License](#license)
- [Contact](#contact)

## Why EnvDataPrep

Environmental researchers work with massive, complex datasets in various formats (NetCDF, HDF5, GRIB). Processing these data requires specialized knowledge of these data structures, programming skills and domain knowledge.

While many optimization techniques can be broadly applied across environmental data processing, domain experts often struggle with performance issues and implementation complexities. EnvDataPrep provides high-performance and research-ready tools that encapsulate optimized exisiting tools and best practices of software engineering, letting scientists focus on discovery rather than data wrangling.

## Current Capacity

*This project is still at the proof-of-concepts stage. An overview of the design is available [here](#design-of-envdataprep).*

The first implemented feature is the **Unified, Configuration-Driven Extraction of NetCDF Files**. This can solve the storage problem for many researchers. Below is an example:
```python
"""An example of the unified, config-driven extraction of NetCDF files."""

import os
import envdataprep as edp

# Setup paths
file_path = "path/to/your_netcdf_file.nc"
output_dir = "output/"

# Selected target variables (using example variables from TROPOMI NO2 products)
target_variables = {
    "latitude": "PRODUCT/latitude",
    "longitude": "PRODUCT/longitude",
    "no2": "PRODUCT/nitrogendioxide_tropospheric_column",
    "cloud_frac": "PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_fraction_crb",
}

if __name__ == "__main__":

    # Discover all variables in the source file
    variables = edp.list_netcdf_variables(file_path)
    print(f"Number of variables: {len(variables)}")
    print(*variables, sep="\n")

    # Create a processor object and extract data
    processor = edp.NetCDFProcessor(file_path, target_variables)
    processor.extract_data()

    # Multiple output options

    # 1) Flat structure, no compression, default output file name
    # The default output file name adds a "SUB" suffix to the input file name
    processor.to_netcdf(output_dir=output_dir)

    # 2) Consistent hierarchical structure with input file, with compression, custom output file name
    processor.to_netcdf(
        out_dir=output_dir,
        output_name="hierarchical_example_compression.nc",
        preserve_groups=True,
        compression='zlib',
        compression_level=6
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
conda env create -f environment.yml
conda activate envdataprep

# 3. Install package in development mode
pip install -e . --no-deps

# 4. Verify installation
python -c "import envdataprep; print('Installation successful!')"
```
**Why development mode for now?**
- Package not yet published to PyPI/conda-forge
- Allows you to get latest features and contribute feedback
- Easy to update with `git pull`

**Note**: We use `pip install -e .` even in conda/mamba environments because:
- **Which pip**: The `pip` command uses the pip from your active conda environment, not system pip
- **Conda/Mamba**: Manages external dependencies (numpy, pandas, netcdf4, etc.)
- **pip -e**: Standard tool for local package development (works perfectly in conda envs)
- **Best practice**: This hybrid approach is widely used in the Python ecosystem

You can verify this with:
```bash
which pip  # Should show: .../miniforge3/envs/envdataprep/bin/pip
```

## Design of EnvDataPrep

### **Extensible Architecture**
- **Open/Closed Principle**: Extensible without modification
- **Template Method Pattern**: Consistent processing workflow
- **Strategy Pattern**: Pluggable performance modes
- **Factory Pattern**: Dynamic processor creation
- **Plugin system**: Add new data sources without modifying core code
- **Configuration-driven**: Extend functionality via config files or scripts
- **Loosely coupled**: Reusable components for maximum flexibility

### **High Performance**
- **Existing Optimized Tools**: NumPy, Xarray, Dask and more
- **Data Structures and Algorithms**: Decisions based on time and space complexity
- **Automatic Optimization**: Basic → Out-of-core → Parallel execution modes
- **Smart Memory Management**: Automatic chunking and memory-efficient processing
- **Parallel Execution**: Built-in support for parallel and distributed computing
- **HPC-Ready**: Seamless scaling from laptop to distributed computing clusters

### **Dual Interface**
- **Python API**
- **CLI Interface**

### **Planned Environmetal Datasets**
- **Satellite Products**: TROPOMI, GEMS, TEMPO
- **Reanalysis Products**: ERA5
- **Forecast Products**: GFS
- **Model Outputs**: WRF, GEOS-Chem
- **Measurements**: Air Quality Network Data

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

- **Author**: Gongda Lu
- **GitHub**: [envmini/envdataprep](https://github.com/envmini/envdataprep)

---

[⬆ Back to top](#envdataprep-extensible-environmental-data-preprocessing-framework)