
  ┌──────┬─────────────────────────────────────┬──────────────┬────────┬────────────┐
  │ Mode │               Formula               │ Output dtype │ Nodata │   Clamp    │
  ├──────┼─────────────────────────────────────┼──────────────┼────────┼────────────┤
  │ -pwr │ likepol / crosspol                  │ float32      │ nan    │ —          │
  ├──────┼─────────────────────────────────────┼──────────────┼────────┼────────────┤
  │ -amp │ amp(likepol) / amp(crosspol) × 1000 │ uint16       │ 0      │ [1, 65535] │
  ├──────┼─────────────────────────────────────┼──────────────┼────────┼────────────┤
  │ -DN  │ DN(likepol) − DN(crosspol) + 127    │ uint8        │ 0      │ [1, 255]   │
  ├──────┼─────────────────────────────────────┼──────────────┼────────┼────────────┤
  │ -dB  │ dB(likepol) − dB(crosspol)          │ float32      │ nan    │ —          │
  └──────┴─────────────────────────────────────┴──────────────┴────────┴────────────┘

  For -amp and -DN, the nodata mask is built before clamping (pixels where the denominator/inputs are 0 or non-finite), then
  clamping is applied to valid pixels, and nodata pixels are set to 0 last — so clamping never accidentally maps valid 0s to 1.

