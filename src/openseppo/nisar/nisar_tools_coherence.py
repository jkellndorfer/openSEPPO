"""
openseppo.nisar.nisar_tools_coherence -- interferometric coherence computation
******************************************************************************
openSEPPO -- Open SEPPO Tools
Supporting Geospatial and Remote Sensing Data Processing

(c) 2026 Earth Big Data LLC  |  https://earthbigdata.com
Licensed under the Apache License, Version 2.0
https://github.com/EarthBigData/openSEPPO

Compute interferometric coherence magnitude between co-registered NISAR GSLC
complex SLC acquisitions.

  γ = |⟨z1·conj(z2)⟩| / sqrt(⟨|z1|²⟩·⟨|z2|²⟩)

Spatial averaging uses a uniform (boxcar) filter.  Inputs must be on the same
grid (same CRS, transform, and shape) — NISAR GSLC products from the same
track/frame/frequency satisfy this automatically.

Input formats
-------------
  * A list of complex64 GeoTIFF files produced by ``seppo_nisar_gslc_convert -cslc``
    (one acquisition per file, band 1).
  * A single multi-band VRT or GeoTIFF where each band is one acquisition
    (e.g. the CSLC time-series VRT built by ``build_track_vrts``).

Nodata convention
-----------------
Pixels where either input is ``0+0j`` are treated as invalid (nodata) and the
coherence output is set to NaN.  Pixels where the denominator averages to zero
(uniform zero power) are also set to NaN.

Memory note
-----------
The coherence computation loads both input arrays into memory simultaneously.
For a 20 m NISAR GSLC full frame (~14 000 × 9 000 pixels, complex64) that is
~2 GB.  Use srcwin/projwin subsets from seppo_nisar_gslc_convert for large
scenes, or process polarisations one at a time.
"""

import os
import gc
import re
import numpy as np
import rasterio
from rasterio.io import MemoryFile


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------


def compute_coherence(z1, z2, window_rows=5, window_cols=5):
    """
    Estimate interferometric coherence between two co-registered complex SLC arrays.

    γ = |⟨z1·conj(z2)⟩| / sqrt(⟨|z1|²⟩·⟨|z2|²⟩)

    Spatial averaging uses a uniform (boxcar) filter of ``window_rows × window_cols``
    pixels (scipy.ndimage.uniform_filter, mode='reflect').

    Parameters
    ----------
    z1, z2 : np.ndarray, complex64, shape (H, W)
        Co-registered complex SLC arrays on the same spatial grid.
        Nodata convention: pixels where real == 0 AND imag == 0 are invalid.
    window_rows, window_cols : int
        Window dimensions.  Typical choices: 5×5 (square), 3×9, 7×3.

    Returns
    -------
    coh : np.ndarray, float32, shape (H, W)
        Coherence magnitude in [0, 1].  NaN where either input is nodata or
        where the power denominator averages to zero.
    """
    from scipy.ndimage import uniform_filter

    if z1.shape != z2.shape:
        raise ValueError(f"Shape mismatch: z1={z1.shape} vs z2={z2.shape}.")

    # Nodata mask: 0+0j convention
    nodata = ((z1.real == 0) & (z1.imag == 0)) | ((z2.real == 0) & (z2.imag == 0))

    z1 = z1.astype(np.complex64)
    z2 = z2.astype(np.complex64)

    ifg = z1 * np.conj(z2)   # complex interferogram
    win = (window_rows, window_cols)

    # |⟨ifg⟩|: filter real and imaginary parts independently then recombine
    avg_r = uniform_filter(ifg.real.astype(np.float64), size=win)
    avg_i = uniform_filter(ifg.imag.astype(np.float64), size=win)
    num = np.sqrt(avg_r * avg_r + avg_i * avg_i)

    # sqrt(⟨|z1|²⟩ · ⟨|z2|²⟩)
    p1 = uniform_filter((z1.real ** 2 + z1.imag ** 2).astype(np.float64), size=win)
    p2 = uniform_filter((z2.real ** 2 + z2.imag ** 2).astype(np.float64), size=win)
    den = np.sqrt(p1 * p2)

    with np.errstate(invalid="ignore", divide="ignore"):
        coh = np.where(den > 0, num / den, np.nan).astype(np.float32)

    coh[nodata] = np.nan
    coh[~np.isfinite(coh)] = np.nan
    return coh


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _write_coh_file(coh, out_path, transform, crs, output_format,
                    metadata=None, output_fs=None, num_threads="ALL_CPUS",
                    float32=False):
    """Write a coherence array as a COG or tiled GeoTIFF.

    Default (float32=False): uint8 DN mode — DN = round(coh * 100), nodata=255.
    float32=True: float32 values in [0, 1], nodata=NaN.
    """
    h, w = coh.shape
    driver = "GTiff" if output_format.upper() == "GTIFF" else "COG"

    if float32:
        arr = coh.astype(np.float32)
        arr[~np.isfinite(arr)] = np.nan
        out_dtype  = "float32"
        out_nodata = float("nan")
        predictor  = 3
    else:
        arr = np.where(np.isfinite(coh),
                       np.clip(np.round(coh * 100), 0, 254).astype(np.uint8),
                       np.uint8(255))
        out_dtype  = "uint8"
        out_nodata = 255
        predictor  = 1

    profile = {
        "driver":      driver,
        "dtype":       out_dtype,
        "count":       1,
        "height":      h,
        "width":       w,
        "crs":         crs,
        "transform":   transform,
        "compress":    "deflate",
        "predictor":   predictor,
        "nodata":      out_nodata,
        "num_threads": str(num_threads),
    }
    if driver == "GTiff":
        profile.update({"tiled": True, "blockxsize": 512, "blockysize": 512,
                        "bigtiff": "IF_SAFER"})
    else:
        profile["overview_resampling"] = "average"

    def _wb(path, raw):
        if path.startswith("s3://"):
            with output_fs.open(path, "wb") as fout:
                fout.write(raw)
        else:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "wb") as fout:
                fout.write(raw)

    with rasterio.Env(GDAL_NUM_THREADS=str(num_threads)):
        with MemoryFile() as memfile:
            with memfile.open(**profile) as dst:
                dst.write(arr, 1)
                if metadata:
                    dst.update_tags(**metadata)
            memfile.seek(0)
            _wb(out_path, memfile.read())


def _parse_nisar_cslc_meta(path):
    """
    Parse NISAR metadata from a -cslc TIF filename produced by
    seppo_nisar_gslc_convert -cslc.

    Expected pattern:
      NISAR_{il}_{pt}_{prod}_{cycle}_{track}_{dir}_{frame}_{mode}_{pol}_{obs}_
      {start_time}_{end_time}_{crid}_{acc}-EBD_{freq}_{pol_str}_cslc.tif

    Returns a dict with all parsed fields, or None if the filename does not
    match the expected NISAR GSLC naming convention.
    """
    basename = os.path.basename(path)
    if "-EBD_" not in basename or not basename.lower().endswith(".tif"):
        return None
    nisar_base, ebd_raw = basename.split("-EBD_", 1)
    ebd_raw = ebd_raw[:-4]          # strip .tif
    ebd_tokens = ebd_raw.split("_") # ["A", "hh", "cslc"]
    if len(ebd_tokens) < 3:
        return None
    tokens = nisar_base.split("_")
    if len(tokens) < 15 or tokens[0] != "NISAR":
        return None
    try:
        return {
            "il":           tokens[1],
            "pt":           tokens[2],
            "prod":         tokens[3],
            "cycle":        int(tokens[4]),
            "track":        int(tokens[5]),
            "direction":    tokens[6],
            "frame":        int(tokens[7]),
            "mode":         tokens[8],
            "polarization": tokens[9],
            "obs_mode":     tokens[10],
            "start_time":   tokens[11],
            "end_time":     tokens[12],
            "crid":         tokens[13],
            "accuracy":     tokens[14],
            "freq":         ebd_tokens[0],
            "pol_str":      ebd_tokens[1],
            "nisar_base":   nisar_base,
        }
    except (IndexError, ValueError):
        return None


def _date_label_from_meta_or_path(meta, path):
    """Return a short YYYY-MM-DD date label from parsed meta or filename."""
    if meta is not None:
        t = meta["start_time"]   # e.g. 20251118T003554
        return f"{t[:4]}-{t[4:6]}-{t[6:8]}"
    basename = os.path.basename(path)
    m = re.search(r"_(\d{8})T\d{6}_", basename)
    if m:
        d = m.group(1)
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return os.path.splitext(basename)[0]


def _build_coh_filename(meta1, meta2, label1, label2, pol, win_r, win_c):
    """
    Build the output filename for a coherence pair.

    When both acquisitions carry full NISAR metadata the filename follows the
    NISAR product convention with product type changed to COH and the two
    acquisition start-times encoded in positions 12–13:

      NISAR_{il}_{pt}_COH_{cycle_ref}-{cycle_sec}_{track:03d}_{direction}_
      {frame:03d}_{mode}_{polarization}_{obs_mode}_{start_ref}_{start_sec}_
      {crid}_{accuracy}-EBD_{freq}_{pol_str}_COH.tif

    Window size is stored in file metadata, not in the filename.
    Falls back to a short generic name when metadata is unavailable.
    """
    pol_lower = pol.lower() if pol else "xx"

    if meta1 is not None and meta2 is not None:
        # Ensure meta1 is the earlier (reference) acquisition
        if meta1["start_time"] > meta2["start_time"]:
            meta1, meta2 = meta2, meta1

        c1, c2 = meta1["cycle"], meta2["cycle"]
        cycle_str = (f"{min(c1,c2):03d}-{max(c1,c2):03d}"
                     if c1 != c2 else f"{c1:03d}")

        return (
            f"NISAR_{meta1['il']}_{meta1['pt']}_COH_{cycle_str}_"
            f"{meta1['track']:03d}_{meta1['direction']}_{meta1['frame']:03d}_"
            f"{meta1['mode']}_{meta1['polarization']}_{meta1['obs_mode']}_"
            f"{meta1['start_time']}_{meta2['start_time']}_"
            f"{meta1['crid']}_{meta1['accuracy']}"
            f"-EBD_{meta1['freq']}_{meta1['pol_str']}_COH.tif"
        )

    if meta1 is not None or meta2 is not None:
        # One side has full NISAR meta; embed the date from the other side
        ref = meta1 if meta1 is not None else meta2
        other_date = label2 if meta1 is not None else label1
        # Convert YYYY-MM-DD label to compact form for the filename
        other_compact = other_date.replace("-", "") + "T000000"
        if ref["start_time"] > other_compact:
            start_ref, start_sec = other_compact, ref["start_time"]
        else:
            start_ref, start_sec = ref["start_time"], other_compact
        return (
            f"NISAR_{ref['il']}_{ref['pt']}_COH_{ref['cycle']:03d}_"
            f"{ref['track']:03d}_{ref['direction']}_{ref['frame']:03d}_"
            f"{ref['mode']}_{ref['polarization']}_{ref['obs_mode']}_"
            f"{start_ref}_{start_sec}_{ref['crid']}_{ref['accuracy']}"
            f"-EBD_{ref['freq']}_{ref['pol_str']}_COH.tif"
        )

    # Fallback: generic name
    pol_tag = f"_{pol_lower}" if pol_lower and pol_lower != "xx" else ""
    return f"coherence{pol_tag}_{label1}_{label2}.tif"


def _parse_nisar_vrt_meta(path):
    """
    Parse NISAR metadata from a time-series VRT (or multi-band TIF) filename
    produced by ``build_track_vrts``.

    Unlike single-acquisition TIF names, the cycle/track/frame tokens may be
    ranges (e.g. ``005-006``, ``999``) and the two timestamp tokens are the
    earliest start and latest end across all acquisitions in the stack.

    Returns a dict or None if the name does not follow the NISAR convention.
    """
    basename = os.path.basename(path)
    # Accept both .vrt and .tif extensions
    stem = re.sub(r"\.(vrt|tif)$", "", basename, flags=re.IGNORECASE)
    if "-EBD_" not in stem or not stem.startswith("NISAR_"):
        return None
    nisar_base, ebd_raw = stem.split("-EBD_", 1)
    ebd_tokens = ebd_raw.split("_")    # e.g. ["A", "hv", "cslc"]
    if len(ebd_tokens) < 3:
        return None
    tokens = nisar_base.split("_")
    if len(tokens) < 13:
        return None
    try:
        cycle_str = tokens[4]           # may be "005" or "005-006"
        min_cycle = int(cycle_str.split("-")[0])
        return {
            "il":           tokens[1],
            "pt":           tokens[2],
            "prod":         tokens[3],
            "cycle_str":    cycle_str,
            "min_cycle":    min_cycle,
            "track_str":    tokens[5],  # may be range or "999"
            "direction":    tokens[6],
            "frame_str":    tokens[7],  # may be range or "999"
            "mode":         tokens[8],
            "polarization": tokens[9],
            "obs_mode":     tokens[10],
            "min_start":    tokens[11], # earliest acquisition start (YYYYMMDDTHHMMSS)
            "max_end":      tokens[12], # latest acquisition end
            "crid":         tokens[13] if len(tokens) > 13 else "",
            "accuracy":     tokens[14] if len(tokens) > 14 else "",
            "freq":         ebd_tokens[0],
            "pol_str":      ebd_tokens[1],
        }
    except (IndexError, ValueError):
        return None


def _infer_cycle(min_cycle, min_date_str, target_date_str, repeat_days=12):
    """
    Estimate the NISAR cycle number for *target_date_str* given a reference
    cycle (*min_cycle*) and its date (*min_date_str*).

    Both date strings may be ``YYYYMMDD``, ``YYYYMMDDTHHMMSS``, or
    ``YYYY-MM-DD``.  Uses the 12-day NISAR repeat period.
    """
    from datetime import datetime
    _clean = lambda s: s[:8].replace("-", "")
    d_min = datetime.strptime(_clean(min_date_str), "%Y%m%d")
    d_tgt = datetime.strptime(_clean(target_date_str), "%Y%m%d")
    return min_cycle + round((d_tgt - d_min).days / repeat_days)


def _build_coh_filename_from_vrt(vrt_meta, label1, label2, win_r, win_c):
    """
    Build a coherence output filename when the input is a multi-band VRT.

    Keeps all NISAR fields from the VRT name, replaces the combined date range
    with the specific pair dates, infers per-pair cycle numbers from the dates
    using the 12-day NISAR repeat period, and replaces the ``-EBD_…_cslc.vrt``
    suffix with ``-EBD_…_COH_win{R}x{C}.tif``.

    Example output for pair (2025-11-18, 2025-11-30) from a VRT named
    ``…_005-006_…_20251118T003554_20251130T003629-EBD_A_hv_cslc.vrt``::

      NISAR_L2_PR_COH_005-006_113_A_013_4005_DHDH_A_20251118T000000_
      20251130T000000_R01234_N-EBD_A_hv_COH_win5x5.tif
    """
    compact1 = label1.replace("-", "")  # YYYYMMDD
    compact2 = label2.replace("-", "")

    # Infer cycle numbers from dates relative to the VRT's min_start
    min_date  = vrt_meta["min_start"][:8]
    min_cycle = vrt_meta["min_cycle"]
    c1 = _infer_cycle(min_cycle, min_date, compact1)
    c2 = _infer_cycle(min_cycle, min_date, compact2)
    cycle_str = (f"{min(c1,c2):03d}-{max(c1,c2):03d}"
                 if c1 != c2 else f"{c1:03d}")

    crid     = vrt_meta["crid"]
    accuracy = vrt_meta["accuracy"]
    if crid and accuracy:
        crid_acc = f"_{crid}_{accuracy}"
    elif crid:
        crid_acc = f"_{crid}"
    else:
        crid_acc = ""

    return (
        f"NISAR_{vrt_meta['il']}_{vrt_meta['pt']}_COH_{cycle_str}_"
        f"{vrt_meta['track_str']}_{vrt_meta['direction']}_{vrt_meta['frame_str']}_"
        f"{vrt_meta['mode']}_{vrt_meta['polarization']}_{vrt_meta['obs_mode']}_"
        f"{compact1}T000000_{compact2}T000000{crid_acc}"
        f"-EBD_{vrt_meta['freq']}_{vrt_meta['pol_str']}_COH.tif"
    )


def _build_env_kwargs(auth):
    """Build rasterio.Env kwargs for S3 access from an auth dict."""
    kw = {
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
        "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.vrt",
    }
    if "profile" in auth:
        kw["AWS_PROFILE"] = auth["profile"]
    elif "key" in auth:
        kw["AWS_ACCESS_KEY_ID"] = auth["key"]
        kw["AWS_SECRET_ACCESS_KEY"] = auth.get("secret", "")
        if "token" in auth:
            kw["AWS_SESSION_TOKEN"] = auth["token"]
    return kw


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def process_coherence_pairs(
    input_paths,
    output_dir,
    window_rows=5,
    window_cols=5,
    pairs="sequential",
    output_format="COG",
    float32=False,
    input_auth=None,
    output_auth=None,
    num_threads=None,
    verbose=False,
):
    """
    Compute pairwise interferometric coherence for NISAR GSLC complex SLC files.

    Parameters
    ----------
    input_paths : list[str] | str
        One of:
          * A list of complex64 GeoTIFF paths (one acquisition per file, band 1).
            Local paths or s3:// URIs.
          * A single path to a multi-band VRT or GeoTIFF where each band is one
            acquisition (band description used as date label).
    output_dir : str
        Output directory (local path or s3:// URI).
    window_rows, window_cols : int
        Coherence estimation window size in pixels.  Default: 5×5.
    pairs : str
        ``"sequential"`` -- pair acquisitions[i] with acquisitions[i+1]  (default).
        ``"all"``        -- all unique pairs i < j.
    output_format : str
        ``"COG"`` (default) or ``"GTiff"``.
    float32 : bool
        If True, write float32 values in [0, 1] with nodata=NaN.
        Default False: write uint8 DN (DN = round(coh * 100), nodata=255).
    input_auth, output_auth : dict | None
        Auth dicts with optional keys ``profile``, ``key``/``secret``/``token``.
    num_threads : int | None
        GDAL compression threads.  None → all available CPUs.
    verbose : bool

    Returns
    -------
    results : list[dict]
        One entry per pair with keys ``success``, ``path``, ``label1``, ``label2``
        (and ``error`` on failure).
    message : str
        Human-readable summary.
    """
    from openseppo.nisar.nisar_tools import create_s3_fs

    if input_auth is None:
        input_auth = {}
    if output_auth is None:
        output_auth = {}

    _n_th = num_threads if num_threads else "ALL_CPUS"
    out_dir = output_dir.rstrip("/")
    _env_kw = _build_env_kwargs(input_auth)

    output_fs = None
    if out_dir.startswith("s3://"):
        output_fs = create_s3_fs(output_auth)

    # --- Build acquisitions list: (label, path, band_idx, pol, meta_or_None) ---
    if isinstance(input_paths, str):
        input_paths = [input_paths]

    acquisitions = []
    vrt_meta = None   # set only for single-file (VRT/multi-band) input

    with rasterio.Env(**_env_kw):
        if len(input_paths) == 1:
            # Single multi-band file or VRT — each band is one acquisition
            src_path = input_paths[0]
            vrt_meta = _parse_nisar_vrt_meta(src_path)
            with rasterio.open(src_path) as src:
                n_bands = src.count
                if n_bands < 2:
                    raise ValueError(
                        f"Single-file mode requires ≥2 bands; "
                        f"{src_path!r} has only {n_bands} band(s).  "
                        f"Pass multiple files or a multi-band VRT."
                    )
                # For a VRT the per-band metadata isn't in the filename;
                # use the band description as the date label.
                base_meta = _parse_nisar_cslc_meta(src_path)
                pol = (vrt_meta["pol_str"].upper() if vrt_meta
                       else (base_meta["pol_str"].upper() if base_meta else ""))
                for b in range(1, n_bands + 1):
                    desc = src.descriptions[b - 1] or f"band{b}"
                    acquisitions.append((desc, src_path, b, pol, None))
        else:
            for p in input_paths:
                meta = _parse_nisar_cslc_meta(p)
                label = _date_label_from_meta_or_path(meta, p)
                pol = meta["pol_str"].upper() if meta else ""
                acquisitions.append((label, p, 1, pol, meta))

    if len(acquisitions) < 2:
        raise ValueError("At least 2 acquisitions are required to compute coherence.")

    # --- Build pair list ---
    n = len(acquisitions)
    if pairs == "all":
        pair_indices = [(i, j) for i in range(n) for j in range(i + 1, n)]
    else:  # sequential
        pair_indices = [(i, i + 1) for i in range(n - 1)]

    if verbose:
        print(f"  {n} acquisition(s), {len(pair_indices)} pair(s), "
              f"window={window_rows}×{window_cols}, format={output_format}", flush=True)

    results = []

    with rasterio.Env(**_env_kw):
        for pair_num, (idx1, idx2) in enumerate(pair_indices, 1):
            label1, path1, band1, pol1, meta1 = acquisitions[idx1]
            label2, path2, band2, pol2, meta2 = acquisitions[idx2]
            pol = pol1 or pol2

            if verbose:
                print(f"  [{pair_num}/{len(pair_indices)}]  {label1}  ×  {label2}",
                      flush=True)

            try:
                # Read first acquisition
                with rasterio.open(path1) as src1:
                    z1 = src1.read(band1)
                    transform = src1.transform
                    crs = src1.crs
                    src_tags = dict(src1.tags())

                # Read second acquisition
                with rasterio.open(path2) as src2:
                    z2 = src2.read(band2)

                if not np.iscomplexobj(z1) or not np.iscomplexobj(z2):
                    raise ValueError(
                        "Input band is not complex.  Use -cslc output files "
                        "(complex64 / CFloat32)."
                    )
                if z1.shape != z2.shape:
                    raise ValueError(
                        f"Shape mismatch: {z1.shape} vs {z2.shape}.  "
                        "Inputs must be on the same grid (same track, frame, frequency)."
                    )

                if verbose:
                    mb = (z1.nbytes + z2.nbytes) / 1e6
                    print(f"    Read {mb:.0f} MB ({z1.shape[1]}×{z1.shape[0]} px), "
                          f"computing coherence ...", flush=True)

                coh = compute_coherence(
                    z1.astype(np.complex64),
                    z2.astype(np.complex64),
                    window_rows=window_rows,
                    window_cols=window_cols,
                )
                del z1, z2
                gc.collect()

                # Output filename
                if vrt_meta is not None:
                    out_name = _build_coh_filename_from_vrt(
                        vrt_meta, label1, label2, window_rows, window_cols,
                    )
                else:
                    out_name = _build_coh_filename(
                        meta1, meta2, label1, label2, pol, window_rows, window_cols,
                    )
                out_path = f"{out_dir}/{out_name}"

                tif_meta = {
                    "PRODUCT":    "coherence",
                    "DATE1":      label1,
                    "DATE2":      label2,
                    "WINDOW":     f"{window_rows}x{window_cols}",
                    "PAIRS_MODE": pairs,
                    "DTYPE":      "float32" if float32 else "uint8_DN",
                    "DN_SCALE":   "N/A" if float32 else "coh = DN / 100  (nodata=255)",
                }
                if pol:
                    tif_meta["POLARIZATION"] = pol
                if meta1 is not None:
                    tif_meta["CYCLE_REF"] = str(meta1["cycle"])
                    tif_meta["TRACK"]     = str(meta1["track"])
                    tif_meta["FRAME"]     = str(meta1["frame"])
                    tif_meta["CRID"]      = meta1["crid"]
                if meta2 is not None:
                    tif_meta["CYCLE_SEC"] = str(meta2["cycle"])
                for k in ("OPENSEPPO_VERSION", "ISCE3_VERSION"):
                    if k in src_tags:
                        tif_meta[k] = src_tags[k]

                _write_coh_file(
                    coh, out_path, transform, crs,
                    output_format=output_format,
                    metadata=tif_meta,
                    output_fs=output_fs,
                    num_threads=_n_th,
                    float32=float32,
                )
                del coh
                gc.collect()

                if verbose:
                    print(f"    -> {out_name}", flush=True)
                results.append({
                    "success": True,
                    "path":    out_path,
                    "label1":  label1,
                    "label2":  label2,
                })

            except Exception as e:
                import traceback
                traceback.print_exc()
                results.append({
                    "success": False,
                    "label1":  label1,
                    "label2":  label2,
                    "error":   str(e),
                })

    ok = sum(1 for r in results if r["success"])
    return results, f"Done: {ok}/{len(results)} coherence map(s) written to {out_dir}/"
