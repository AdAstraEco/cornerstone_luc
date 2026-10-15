# `docs/orbae/`: AdAstra's notes on this pipeline

Everything in this folder is ours. Nothing here exists on `upstream/main` (`cornerstone-data/luc`). The files in `docs/` one level up are the ones we share with Cornerstone, and this folder exists to keep the two apart.

It covers three things: the emissions layer Orbae needs and how much of it the pipeline now provides; how the pipeline compares to Orbae's draft specification; and how we run the pipeline in the cloud.

______________________________________________________________________

## 1 · The Orbae emissions layer

### [`orbae_emissions_data_model.md`](orbae_emissions_data_model.md)

**The design target.** Orbae's proposed data model: a crop-independent layer 1, computed once per reference year; a crop-specific layer 2 derived from it; and the client deliverable, which is just the total. Each row comes with its justification.

### [`overall_data_flow.md`](overall_data_flow.md)

**The picture.** One Mermaid chart: the repo's stages, the Orbae layers, and the alignment point where `emit`'s output meets layer 1. Run `tools/strip-mermaid-detail.py` on it for a headings-only version.

### [`crop_independent_emissions.md`](crop_independent_emissions.md)

**Layer 1 as built.** `emit`'s `cie-` bands next to its existing bands, one row per quantity, and how each existing band is derived from the `cie-` ones (`emit.derive_from_cie`). The code is `emit.get_crop_independent_emissions`. To compare the two on a real tile, run [`../../tools/compare-emit-layers.py`](../../tools/compare-emit-layers.py).

### [`jdluc_derivability.md`](jdluc_derivability.md)

**From layer 2 to jdLUC.** A parsimonious "new jdLUC" (one row per ADM3, extensive values only), the decisions it forces on the data model, how legacy jdLUC derives from it, and which annual columns are worth keeping.

______________________________________________________________________

## 2 · The pipeline against the spec

### [`orbae_emissions_layer_spec.md`](orbae_emissions_layer_spec.md)

**Not our document, and not current.** A frozen snapshot (`0db3abd`) of Orbae's draft emissions-layer specification, kept so the review below has a fixed thing to point at. Its banner links to the live version.

### [`orbae_spec_alignment.md`](orbae_spec_alignment.md)

**The main analysis.** A three-way comparison of the spec, the repo's documentation and what `jdluc/` actually does, organised by pipeline stage. Its headline: alignment is broader than the spec's own "Declared differences" suggest, and most of the significant disagreements resolve with one publishing change at `emit`. That change has since been made (the `cie-` bands). The box at the top says which findings it resolved and which are still open.

### [`stage-contents.md`](stage-contents.md)

**What each stage holds:** 57 bands out of harmonize, 20 + 10 `cie-` out of emit, about 12 columns out of attribute. Its "What is lost, and where" table traces what each stage stops carrying. Its footnotes record traps found while reading the code: silent-zero lookups, an unresolved SoilGrids scale factor, and `ASSESSMENT_YEAR` sitting outside the cache key. Its *Storage* section holds the open question of how much of the footprint is inherent.

______________________________________________________________________

## 3 · Running it in the cloud

### [`argo-workflows.md`](argo-workflows.md)

**How we run it.** The pipeline as an Argo Workflow DAG, one pod per tile per phase. Covers the design choices behind that shape, one-off setup, how to submit and watch a country, outputs, measured timings, pod sizing and gotchas. Start here for anything about running the pipeline.

### [`scaling-reliability-assessment.md`](scaling-reliability-assessment.md): report, 2026-09

The feasibility spike's assessment, written for a build-on-this-or-not decision, and based on a 2-tile Czechia run. The architecture is sound and cheap at 30 m: a whole-world land run is 279 tiles and tens of dollars of compute. The risk is not compute or reliability but **storage cost, and how it scales with resolution**. It predates Argo and the migration; its header explains what that changes.

### [`gke-disk-io-findings.md`](gke-disk-io-findings.md): report, 2026-09

Research into the disk-I/O stall the spike hit: why GDAL's temp writes throttled to a crawl on a shared boot disk, and the options for fast per-pod scratch space. We chose a per-pod pd-ssd volume instead of its Local SSD recommendation; its header says why.

______________________________________________________________________

## Loose ends

- **Storage cost has not been measured.** See *Storage* in `stage-contents.md`, and the retention question in `argo-workflows.md`.
- **Ingest isn't versioned** by source-dataset version (an open question in `argo-workflows.md`).
- The spec comparison is pinned to repo state `8130655` and spec state `0db3abd`. Only its "since" box tracks later changes.
