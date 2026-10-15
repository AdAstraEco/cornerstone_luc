# `docs/orbae/` — AdAstra working notes

Everything in this folder is ours. Nothing here exists on `upstream/main`
(`cornerstone-data/luc`); the files in `docs/` one level up are the ones we share with
Cornerstone, and keeping the two apart is the only reason this folder exists.

These are **working documents from an evaluation of this repository**, not deliverables
Cornerstone has agreed to. They were written to answer three questions, and they divide
cleanly along those lines:

1. **What does the pipeline actually do?** — reading the code closely enough to argue with it.
2. **How does it compare to the Orbae emissions-layer spec?** — where the two agree, where
   they disagree, and what it would cost to close the gaps.
3. **Will it run and scale in our infrastructure, and what will it cost?**

Dates below are when each document was written or last substantially revised — not file
timestamps, several of which were reset by branch operations.

---

## 1 · Understanding the pipeline

### [`data_flow.md`](data_flow.md) — 2026-09-22

**Working note.** What each stage of the pipeline *holds*: 57 bands out of harmonize, 20 out
of emit, ~12 columns by attribute. Verified band-by-band against commit `8130655`. Companion
to [`../architecture.md`](../architecture.md), which covers the stage *sequence* — this one is
about width.

The payload is the "What is lost, and where" table and its footnotes, which trace what each
stage stops carrying and whether it is recoverable. Its conclusion: only one genuine
information loss exists in the pipeline (a source class where no destination resolved
collapses to `NONE`); everything else is either recoverable upstream or reconstructible from
what is published. The footnotes also record several traps found while reading —
silent-zero lookups, an unresolved SoilGrids scale factor, and `ASSESSMENT_YEAR` being a
module constant outside the cache key, so changing it and re-running silently returns the old
zarr.

### [`crop_independent_emissions.md`](crop_independent_emissions.md)

emit's existing bands next to its `cie-` (Crop-Independent Emissions) bands, one row per
quantity, and how each existing band is derived from the `cie-` ones. The code is
`emit.get_crop_independent_emissions`. For a numeric comparison on one tile, run
[`../../tools/compare-emit-layers.py`](../../tools/compare-emit-layers.py).

---

## 2 · The Orbae spec comparison

Read the spec first if you are new to it, then pick whichever of the two comparisons suits
how you work: the alignment review argues a case, the side-by-side lets you check the
quotations yourself.

### [`orbae_emissions_layer_spec.md`](orbae_emissions_layer_spec.md) — imported 2026-09-22

**Not our document, and not current.** A snapshot of Orbae's draft emissions-layer
specification, committed here purely so the two comparisons below have a fixed thing to point
at. It carries its own warning banner and a link to the live Google Doc. **Do not treat this
copy as a reference** — it is an object of discussion, frozen at commit `0db3abd`.

Eight calculation steps (event sources → value sources → conversion event → peat/soil regime →
emissions per event → reference year → filters → export), plus the spec's own "Declared
differences to the Cornerstone proof of concept".

### [`orbae_spec_alignment.md`](orbae_spec_alignment.md) — 2026-09-22

**The main analytical document, and the longest.** A three-way comparison of the spec, the
repo's own documentation, and what `jdluc/` actually does — deliberately kept as three
corners rather than the usual two, because the repo's docs and its code do not always agree.

Organised by the repo's five-stage pipeline rather than the spec's eight steps, with a map
between the two at the top. Sections: stage-by-stage walkthrough; five structural findings;
corrections to the spec (claims that were never true, and claims overtaken by the `8130655`
migration); shared blind spots; and a table of places where the repo's documentation and its
code disagree.

Its headline: alignment is **broader than the spec's own "Declared differences" suggest**, and
five of the significant disagreements — the destination class being read and required, the
peat-occupation gate, reference-year dependence, unmaterialised intermediates, and five-year
spans — largely resolve by the same single change at the emit stage. The soil-variant
disagreement does *not* come with it, and is called out as needing separate scoping because it
needs factors the repo does not hold at all.

### [`orbae_spec_sidebyside.html`](orbae_spec_sidebyside.html) — 2026-09-22

The same comparison rendered as a **quotation-level table, ordered by the spec's own sequence**
rather than the repo's. 56 points compared, each classified: 17 aligned, 2 convergent, 6
partial, 24 disagree, 7 unbuilt. Left column carries the identifier and verdict, middle quotes
the spec, right quotes the repo's documentation — and says so plainly where the repo has
nothing to say on the point.

Use this when you want to check a specific claim against the source text. Use the alignment
review when you want the argument.

---

## 3 · Running it in our infrastructure

### [`argo-workflows.md`](argo-workflows.md) — 2026-10-15

**How we run it now.** The pipeline as an Argo Workflow DAG, one pod per tile per phase, with
the tile list derived from the country in-cluster. From the user's side: the pieces, how to
submit and watch a country, where outputs land, measured timings, and the gotchas. Supersedes
the hand-rolled driver in `scale-out.md`. Start here for anything about running the pipeline
today.

### History: how we got here

The notes below record the exploration that led to Argo, in the order it happened. They are
kept for their reasoning and measurements; none of them describes how we run the pipeline now.

#### [`cornerstone-k8s.md`](cornerstone-k8s.md) — 2026-09-10

**History: the feasibility spike's proposal; nothing was deployed when this was written.** How
to run the pipeline in our GKE cluster as one Kubernetes Job per 10° tile — no Dask scheduler,
no change to `jdluc`. Most of the scaffolding it describes has since been removed; the image
(`infra/Dockerfile`) and the per-pod entry point (`infra/run_phase.py`) survive under Argo.

The reasoning worth reading is the "Why Jobs, not a Dask cluster" section: the goal was to
de-risk *the repository*, so anything that also required us to evaluate and operate Dask would
let a Dask failure masquerade as a repo failure. Also lists the integration risks found by
reading the code first — the cache key needing `.git` in the image, `.env` being a file rather
than environment, and caching that can make a scaling run finish instantly and *look* like it
scaled.

#### [`scaling-reliability-assessment.md`](scaling-reliability-assessment.md) — 2026-09-11

**History: the spike's finished assessment, written for a build-on-this-or-not decision.** Its
findings still stand; it predates Argo. All four phases run
end-to-end on GKE against real country data. Deliberately gives as much weight to the failure
modes we did *not* hit as to the one we did.

Findings: the architecture is sound and cheap at 30 m — a whole-world land run is 279 tiles,
tens of dollars, ~110 pod-hours. The one failure (disk I/O) was diagnosed and fixed, turning a
45-minute stall into 9.5 minutes, and retry-safe caching makes a warm re-run a near-no-op
(39 s measured). **The risk is not compute or reliability — it is storage cost, and how it
scales with resolution** (~9× on a 30 m → 10 m move).

Read §2 before the numbers. The evidence is a live 2-tile Czechia run plus an earlier Honduras
run: a functional pilot, not a scale test. Per-tile compute can plausibly swing ±2× across the
globe, so whole-world figures are order-of-magnitude. §10 splits the conclusions into solid
today / solid with moderate work / unproven.

#### [`scale-out.md`](scale-out.md)

**History: the hand-rolled driver.** Chose one Indexed Job per phase, driven by a script, over
Argo, and listed the triggers for revisiting that choice. Those triggers have been hit, and
Argo has replaced the driver. Still the reference for the options weighed and for *Storage,
not compute, is the first scale risk*.

#### [`ingest-compute-separation.md`](ingest-compute-separation.md)

**Design note** on splitting ingest from compute: `ingest-world` / `ingest-tiles` ahead of a
`compute` that runs `--skip-ingest` against the warm cache, without the API keys. Implemented
first by the driver and now by the Argo workflow. Versioning (its item 4) is not done.

### [`storage-cost-brief.md`](storage-cost-brief.md) — 2026-09-10

**A brief, not a report** — the commissioning document for the storage investigation the
assessment above identified as the highest-value follow-up. Sets the core question (how much
of the TiB-scale intermediate footprint is inherent to the architecture versus an artifact of
current encoding choices), records the facts already verified against the local Honduras run,
and lays out six lines of enquiry: zarr encoding, dtypes, redundancy across emit's 20
variables, the architectural floor, non-data mitigations, and 30 m → 10 m sensitivity.

Its deliverable, `docs/storage-cost-findings.md`, **has not been written.** The brief insists
on measurements rather than estimates wherever local data allows.

---

## Suggested reading order

Skimming for the conclusions: `scaling-reliability-assessment.md` §1 and §10, then
`orbae_spec_alignment.md` §5.

Running the pipeline in the cloud: `argo-workflows.md` alone.

Reading properly, and in roughly the order the thinking happened:
`cornerstone-k8s.md` → `scaling-reliability-assessment.md` → `scale-out.md` →
`ingest-compute-separation.md` → `argo-workflows.md` for the infrastructure, then
`storage-cost-brief.md` → `data_flow.md` → `orbae_spec_alignment.md`, with the spec and the
side-by-side open alongside the last of these.

## Loose ends

- **`docs/storage-cost-findings.md` does not exist.** `storage-cost-brief.md` commissions it.
- The comparisons are pinned to a repo state of `8130655` (the GLCLUC → TCL/GPW/GACED30
  migration) and a spec state of `0db3abd`. Both go stale as either side moves.
