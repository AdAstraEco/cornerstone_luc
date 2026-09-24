# Spike: Crop-Independent Emissions in `emit`

Working note, 2026-09-24, branch `spike/orbae-layer1`. This is a first implementation of Crop-Independent Emissions (CIE), the non-crop-specific layer in [`orbae_emissions_data_model.md`](orbae_emissions_data_model.md) §1. The new bands are added **alongside** the existing ones in the same `emit` zarr, so the old and new outputs can be compared. The code is `emit.get_crop_independent_emissions`, and the comparison is done with `tools/compare-emit-layers.py`.

## What is emitted

The new bands have a `cie-` prefix. `emit`'s cache version goes from 1 to 2. Downstream readers (`statistical`, `jurisdictional_direct`, `trace`) select bands by name, so they are unaffected.

| band                                                          | how it is computed                                                             | differs from the old bands by                                         |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| `cie-conversion-source`                                       | `NONE 0 · FOREST 1 · NATURAL_GRASSLAND 2 · PASTURE 3`, from the `from_*` masks | not gated on destination                                              |
| `cie-conversion-year`                                         | `ConversionRecord.year`, 0 where there is no source                            | same as the old value wherever a source exists; the gate is defensive |
| `cie-mineral-soil-carbon-at-risk:tco2e-per-ha`                | SOC × 44/12, 0 on peat                                                         | no Table 5.5 loss fraction and no destination gate                    |
| `cie-peat-transformation-emissions-undiscounted:tco2e-per-ha` | 621 × peat                                                                     | not gated on destination                                              |
| `cie-vegetation-emissions-undiscounted:tco2e-per-ha`          | forest or grassland carbon × 44/12, by source                                  | not gated on destination                                              |
| `cie-peat-occupation-emissions:tco2e-per-ha-per-year`         | 37.3 × peat, **on all peat**                                                   | not gated on conversion or destination                                |
| `cie-climate-zone`                                            | harmonized band                                                                | passed straight through                                               |
| `cie-continent`                                               | all NaN                                                                        | no source yet                                                         |
| `cie-hectares-per-pixel:ha`, `cie-destination-dataset`        | copies                                                                         | none; duplicated so the `cie-` set stands on its own                  |

**Not emitted:** the y-20 land-use bitmask (see below).

**Identities between old and new.** These are tested in `emit_test.py` and checked per tile by the tool:

- Where a conversion fired:
  - old vegetation summed over spans = `cie-vegetation`;
  - old soil summed over spans = `cie-peat` + (1 − retention) × `cie-mineral` for cropland destinations, or `cie-peat` alone for pasture destinations;
  - the years match.
- Where a source exists but no destination claimed the pixel: the old bands charge nothing, and `dropped-emissions` equals `cie-vegetation`, linearly discounted. CIE carries these pixels in full.

## Upstream changes required

1. **Conversion record.** `ConversionRecord` gains a `source` field. The masks already existed; they just weren't saved before the destination gate.
2. **Mineral soil factor.** The cropland FLU factor is currently baked into `CLIMATE_ZONE_TO_SOC_RETENTION_FRACTION`. CIE needs the raw stock. Layer 2 then needs a crop × climate-zone FLU table, but today only two cases exist: cropland, and pasture (a factor of 1.0).
3. **Continent.** No dataset in the pipeline provides it. One option is a country → continent raster in harmonize, for example rasterized `worldbank_jurisdictions`.
4. **Land use in y-20.** GPW 2000 (natural grassland, pasture) and GACED30 2000 (cropland) are already harmonized. The forest bit needs Hansen `treecover2000` plus a canopy threshold. Only `lossyear` is ingested today.
5. **Reference year.** `ASSESSMENT_YEAR = 2020` is a module constant, and the lookback window and destination predicates hang off it. CIE is undiscounted, so it doesn't use the span table, but one layer per reference year still means making that year a parameter of `emit.workflow`.
6. **Output types.** `geo.unify_dtype_and_no_data` casts every band to float32, so the enum and bitmask bands are floats on disk. Each tile goes from 20 bands to 30.

## Status

- Unit tests pass.
- The tool has been checked end to end on a synthetic tile only. The emit caches on local disk predate the current dataset stack (GLAD GLCLUC instead of GPW and GACED30). A real run (CIV or CZ) first needs GPW, GACED30, TCL and Descals ingested.
