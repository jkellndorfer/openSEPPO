# Quick Start

Get from zero to NISAR GeoTIFFs in three commands.

---

## 1 — Install

```bash
mamba create -n openseppo -c conda-forge openseppo aria2
conda activate openseppo
```

---

## 2 — Credentials

Store your [NASA Earthdata](https://urs.earthdata.nasa.gov/users/new) login in `~/.netrc` once:

```
machine urs.earthdata.nasa.gov
    login <your_username>
    password <your_password>
```

Then pre-cache the bearer token:

```bash
seppo_earthaccess_credentials -t
```

---

## 3 — Find scenes

Search for all available NISAR GCOV scenes over Los Angeles:

```bash
seppo_nisar_search --point -118.24 34.05 --buffer 1.0 -o urls.txt
```

`urls.txt` now contains one `s3://` URL per line, one scene per row.

---

## 4 — Convert to GeoTIFF

Convert all scenes in `urls.txt` to dB-scaled Cloud Optimized GeoTIFFs and build a time-series VRT stack:

```bash
seppo_nisar_gcov_convert -i urls.txt -o out/ -dB -v
```

Output in `out/`:

```
out/
  NISAR_..._hh_dB.tif       ← HH backscatter (float32, dB, COG)
  NISAR_..._hv_dB.tif       ← HV backscatter (float32, dB, COG)
  NISAR_..._hhhv_dB.vrt     ← per-scene snapshot VRT (both pols)
  NISAR_..._hh_dB.vrt       ← time-series VRT stack (all dates, 1 band = 1 date)
  NISAR_..._hv_dB.vrt        ← time-series VRT stack
```

Open the time-series VRT directly in QGIS or with `xarray`:

```python
import xarray as xr
ds = xr.open_dataset("out/NISAR_..._hh_dB.vrt", engine="rasterio")
```

---

## Common variants

```bash
# Compact uint8 browse image + dual-pol ratio, 100 m resolution, WGS84
seppo_nisar_gcov_convert -i urls.txt -o out/ \
    -DN -dpratio --no_single_bands \
    -t_srs 4326 -tr 0.001 0.001

# Single local HDF5 file, amplitude uint16
seppo_nisar_gcov_convert -i scene.h5 -o out/ -amp

# Direct to S3
seppo_nisar_gcov_convert -i urls.txt -o s3://my-bucket/nisar/ -dB
```

---

See [Examples](nisar_gcov_convert_examples.md) for the full reference, or [CLI Reference](nisar_gcov_convert_cli.md) for all options.
