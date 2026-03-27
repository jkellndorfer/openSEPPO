# seppo_nisar_coherence -- CLI Reference

Compute pairwise interferometric coherence from co-registered NISAR GSLC complex SLC data.

---

## Overview

```
gamma = |<z1 * conj(z2)>| / sqrt(<|z1|^2> * <|z2|^2>)
```

Spatial averaging uses a uniform boxcar window (default 5×5 pixels). Inputs must be **complex64 (CFloat32) GeoTIFFs** produced by `seppo_nisar_gslc_convert -cslc`, or a multi-band VRT/TIF where each band is one acquisition. NISAR GSLC products from the same track/frame are already co-registered and geocoded to a common grid — no additional registration is needed.

Output is uint8 DN by default (`DN = round(coh × 200)`, nodata=255), or float32 in [0, 1] with `--no_DN`.

---

## Usage

```
seppo_nisar_coherence [-h] -i INPUT [INPUT ...] -o OUTPUT
                      [-window N [N ...]] [-pairs {sequential,all}]
                      [-of {COG,GTiff}] [-no_DN]
                      [-projwin ULX ULY LRX LRY] [-projwin_srs CRS]
                      [-d N [N ...]] [-t_srs T_SRS] [-tr RES [RES ...]]
                      [--profile PROFILE] [--input_profile INPUT_PROFILE]
                      [--output_profile OUTPUT_PROFILE]
                      [-no_vrt] [-v]
```

---

## Arguments

### Input / Output

| Argument | Description |
|----------|-------------|
| `-i`, `--input` | Input complex64 GeoTIFF file(s) or a single multi-band VRT/TIF. Multiple TIFs: band 1 of each file is one acquisition. Single VRT/TIF: each band is one acquisition, band description used as date label. Paths may be local or `s3://`. |
| `-o`, `--output` | Output directory for coherence maps (local path or `s3://` URI). |
| `-of {COG,GTiff}` | Output raster format. Default: `COG`. |

### Coherence Options

| Argument | Description |
|----------|-------------|
| `-window N [N ...]` | Coherence estimation window: one integer (square) or two integers (rows cols). Default: `5 5`. Larger windows reduce noise but smooth edges. For 20 MHz GSLC data (10 m × 5 m pixels), `-window 3 9` gives roughly equal ground coverage in both directions; for 40 MHz data (5 m × 5 m) a square window is appropriate. |
| `-pairs {sequential,all}` | Pairing strategy. `sequential` (default): pair acquisitions i with i+1, giving N−1 pairs. `all`: every unique pair i<j, giving N×(N−1)/2 pairs. |

### Output DN Encoding

| Argument | Description |
|----------|-------------|
| `-no_DN`, `--no_DN_8bit` | Write coherence as float32 in [0, 1] with nodata=NaN instead of the default uint8 encoding. |

Default uint8 encoding: `DN = round(coh × 200)`, nodata=255. To recover coherence from DN: `coh = DN / 200`.

### Post-Processing: Crop / Downscale / Reproject

Post-processing is applied in this order: **crop → downscale → reproject**.

| Argument | Description |
|----------|-------------|
| `-projwin ULX ULY LRX LRY` | Crop output to this bounding box (applied after coherence estimation). `ulx uly lrx lry`. Coordinates are in the native CRS unless `-projwin_srs` is given. |
| `-projwin_srs CRS` | CRS of the `-projwin` coordinates (e.g. `EPSG:4326` or bare `4326`). If omitted, `-projwin` is assumed to be in the native raster CRS. When combined with `-t_srs`, the window is reprojected via the target CRS first to ensure the crop is large enough to fill the full reprojected output. |
| `-d N [N ...]`, `--downscale` | Block-average downscale factor applied after crop. One integer for isotropic (e.g. `-d 2`) or two integers **X Y** for anisotropic (columns then rows, e.g. `-d 2 4`). |
| `-t_srs T_SRS` | Output CRS for reprojection (e.g. `EPSG:4326`, `WGS84`, or PROJ string). Coherence is resampled with bilinear interpolation. |
| `-tr RES [RES ...]` | Output pixel size in target CRS map units. One value (square) or two values X Y. Can be combined with `-t_srs`. |

### AWS / Cloud

| Argument | Description |
|----------|-------------|
| `--profile` | AWS profile for both input and output. |
| `--input_profile` | AWS profile for reading input files (overrides `--profile`). |
| `--output_profile` | AWS profile for writing output files (overrides `--profile`). |

### VRT

| Argument | Description |
|----------|-------------|
| `-no_vrt`, `--no_vrt` | Disable building a time-series VRT stacking all coherence pairs. |

### General

| Argument | Description |
|----------|-------------|
| `-v`, `--verbose` | Verbose output. |
| `-h`, `--help` | Show help and exit. |

---

## Output Files

For each pair, one coherence TIF is written:

```
NISAR_..._HH_COH_w05x05_20251119_20260118.tif
```

The filename encodes: mission, product metadata, polarization, `COH`, window size, date 1, date 2.

A time-series VRT stacking all pairs is also created (unless `--no_vrt`):

```
NISAR_..._HH_COH_w05x05.vrt    <- one band per pair
```

---

## Window Size Guidance

Native pixel spacing varies by bandwidth. A square coherence window covers asymmetric ground area for non-square pixels:

| Bandwidth | Pixel spacing (X × Y) | Square window 5×5 covers | Recommended window |
|-----------|-----------------------|--------------------------|-------------------|
| 40 MHz | 5 m × 5 m | 25 m × 25 m | `-window 5 5` |
| 20 MHz | 10 m × 5 m | 50 m × 25 m | `-window 3 9` (~30 m × 27 m) |

```bash
# 20 MHz: 3×9 window gives roughly square ground coverage
seppo_nisar_coherence -i *.tif -o out/ -window 3 9
```

---

## Usage Examples

```bash
# 1. Sequential pairs from a list of -cslc TIFs
seppo_nisar_coherence -i a_hh.tif b_hh.tif c_hh.tif -o out/

# 2. All pairs from a multi-band CSLC time-series VRT
seppo_nisar_coherence -i ts_HH_cslc.vrt -o out/ -pairs all

# 3. Custom window (3 rows × 9 cols, nearly square at 20 MHz)
seppo_nisar_coherence -i a.tif b.tif -o out/ -window 3 9

# 4. Float32 output (no DN encoding)
seppo_nisar_coherence -i a.tif b.tif -o out/ -no_DN

# 5. Crop to a bounding box (native UTM coordinates)
seppo_nisar_coherence -i *.tif -o out/ \
    -projwin 847242 2570282 892239 2527678

# 6. Crop in lon/lat, then downscale 2x
seppo_nisar_coherence -i *.tif -o out/ \
    -projwin 72.39 23.21 72.82 22.81 -projwin_srs EPSG:4326 \
    -d 2

# 7. Anisotropic downscale (2x cols, 4x rows)
seppo_nisar_coherence -i *.tif -o out/ -d 2 4

# 8. Reproject to geographic coordinates at 0.0002°
seppo_nisar_coherence -i *.tif -o out/ -t_srs EPSG:4326 -tr 0.0002

# 9. Crop in lon/lat + reproject to geographic
seppo_nisar_coherence -i *.tif -o out/ \
    -projwin 72.39 23.21 72.82 22.81 -projwin_srs EPSG:4326 \
    -t_srs EPSG:4326 -tr 0.0002

# 10. Write to S3
seppo_nisar_coherence -i *.tif -o s3://my-bucket/coherence/ -v

# 11. No VRT, float32 output
seppo_nisar_coherence -i a.tif b.tif -o out/ -no_DN -no_vrt
```
