"""Synthetic (dummy) netCDF-style xarray datasets for tests, docs, and demos.

Not for operational data pipelines — use :mod:`envdataprep.core.netcdf` for that.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

from ..core.netcdf.write import write_netcdf


def make_sample_flat_nc_dataset(
    n_time: int = 4,
    n_level: int = 3,
    n_lat: int = 5,
    n_lon: int = 7,
    *,
    seed: int | None = 42,
) -> xr.Dataset:
    """Build a **synthetic** flat (no groups) grid with time, level, lat, lon.

    For tests, examples, and learning — not real observations.

    Variables:

    - ``temperature`` — ``(time, level, latitude, longitude)``
    - ``surface_pressure`` — ``(time, latitude, longitude)``

    Coordinate ``level`` uses nominal hPa values. ``time`` uses a simple
    floating offset in hours with CF-style ``units`` on the coordinate.

    Parameters
    ----------
    n_time : int, default 4
        Number of time steps.
    n_level : int, default 3
        Number of vertical levels.
    n_lat : int, default 5
        Number of latitude points.
    n_lon : int, default 7
        Number of longitude points.
    seed : int, optional
        RNG seed for stochastic parts; ``None`` for non-deterministic.

    Returns
    -------
    xarray.Dataset
    """
    rng = np.random.default_rng(seed)
    hours = np.arange(n_time, dtype=np.float64)
    level = np.linspace(1000.0, 300.0, n_level, dtype=np.float32)
    lat = np.linspace(-60.0, 60.0, n_lat, dtype=np.float32)
    lon = np.linspace(-120.0, 120.0, n_lon, dtype=np.float32)

    lat2 = lat[:, None]
    lon2 = lon[None, :]
    base_2d = (
        280.0
        + 15.0 * np.sin(np.pi * lat2 / 90.0) * np.cos(np.pi * lon2 / 180.0)
    ).astype(np.float32)

    t_w = hours[:, None, None, None]
    lev_w = (level / 500.0)[None, :, None, None]
    base_4d = base_2d[None, None, :, :] * (1.0 + 0.01 * t_w) * lev_w
    noise = rng.standard_normal(
        (n_time, n_level, n_lat, n_lon), dtype=np.float32,
    )
    temperature = (base_4d + noise * 0.5).astype(np.float32)

    sp_noise = rng.standard_normal((n_time, n_lat, n_lon), dtype=np.float32)
    surface_pressure = (1013.0 + sp_noise * 2.0).astype(np.float32)

    return xr.Dataset(
        coords={
            "time": (
                "time",
                hours,
                {
                    "units": "hours since 2020-01-01 00:00:00",
                    "calendar": "standard",
                    "long_name": "time",
                },
            ),
            "level": (
                "level",
                level,
                {
                    "units": "hPa",
                    "long_name": "air_pressure",
                    "positive": "down",
                },
            ),
            "latitude": (
                "latitude",
                lat,
                {"units": "degrees_north", "standard_name": "latitude"},
            ),
            "longitude": (
                "longitude",
                lon,
                {"units": "degrees_east", "standard_name": "longitude"},
            ),
        },
        data_vars={
            "temperature": (
                ["time", "level", "latitude", "longitude"],
                temperature,
                {
                    "long_name": "air temperature",
                    "units": "K",
                    "_FillValue": np.float32(np.nan),
                },
            ),
            "surface_pressure": (
                ["time", "latitude", "longitude"],
                surface_pressure,
                {"long_name": "surface pressure", "units": "hPa"},
            ),
        },
        attrs={
            "title": "envdataprep synthetic flat nc",
            "source": "envdataprep.sample_data",
            "Conventions": "CF-1.10",
        },
    )


def make_sample_grouped_nc_dataset(
    n_time: int = 4,
    n_row: int = 6,
    n_col: int = 8,
    *,
    seed: int | None = 42,
) -> xr.Dataset:
    """Build a **synthetic** grouped-path dataset (``PRODUCT/...``).

    Dimensions ``time`` × ``row`` × ``col``. For tests and demos only.

    ``PRODUCT/latitude`` and ``PRODUCT/longitude`` stay 2D ``(row, col)``.
    ``PRODUCT/radiance`` is ``(time, row, col)``.

    Parameters
    ----------
    n_time : int, default 4
        Number of time steps (integer index coordinate).
    n_row : int, default 6
        Scan-line dimension size.
    n_col : int, default 8
        Pixel dimension size.
    seed : int, optional
        RNG seed for ``radiance``.

    Returns
    -------
    xarray.Dataset
    """
    rng = np.random.default_rng(seed)
    time = np.arange(n_time, dtype=np.int32)
    row = np.arange(n_row, dtype=np.int32)
    col = np.arange(n_col, dtype=np.int32)
    lat2d = np.tile(
        np.linspace(-55.0, 55.0, n_row, dtype=np.float32), (n_col, 1)
    ).T
    lon2d = np.tile(
        np.linspace(-100.0, 100.0, n_col, dtype=np.float32), (n_row, 1)
    )
    rad = rng.uniform(
        0.0, 100.0, size=(n_time, n_row, n_col),
    ).astype(np.float32)

    return xr.Dataset(
        coords={
            "time": ("time", time, {"long_name": "observation index"}),
            "row": ("row", row, {"long_name": "scan line"}),
            "col": ("col", col, {"long_name": "pixel"}),
        },
        data_vars={
            "PRODUCT/latitude": (
                ["row", "col"],
                lat2d,
                {"units": "degrees_north", "long_name": "latitude"},
            ),
            "PRODUCT/longitude": (
                ["row", "col"],
                lon2d,
                {"units": "degrees_east", "long_name": "longitude"},
            ),
            "PRODUCT/radiance": (
                ["time", "row", "col"],
                rad,
                {
                    "units": "W m-2 sr-1 nm-1",
                    "long_name": "TOA radiance",
                },
            ),
        },
        attrs={
            "title": "envdataprep synthetic grouped nc",
            "source": "envdataprep.sample_data",
            "sensor": "SYNTHETIC",
        },
    )


def write_sample_flat_nc(
    output_path: str,
    n_time: int = 4,
    n_level: int = 3,
    n_lat: int = 5,
    n_lon: int = 7,
    *,
    seed: int | None = 42,
    compression: str | None = None,
    **kwargs,
) -> str:
    """Write :func:`make_sample_flat_nc_dataset` to disk (synthetic data only)."""
    ds = make_sample_flat_nc_dataset(
        n_time=n_time,
        n_level=n_level,
        n_lat=n_lat,
        n_lon=n_lon,
        seed=seed,
    )
    write_netcdf(ds, output_path, compression=compression, **kwargs)
    return output_path


def write_sample_grouped_nc(
    output_path: str,
    n_time: int = 4,
    n_row: int = 6,
    n_col: int = 8,
    *,
    seed: int | None = 42,
    compression: str | None = None,
    **kwargs,
) -> str:
    """Write :func:`make_sample_grouped_nc_dataset` to disk (synthetic only)."""
    ds = make_sample_grouped_nc_dataset(
        n_time=n_time,
        n_row=n_row,
        n_col=n_col,
        seed=seed,
    )
    write_netcdf(ds, output_path, compression=compression, **kwargs)
    return output_path
