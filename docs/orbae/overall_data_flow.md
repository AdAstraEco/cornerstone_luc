# Overall data flow — repo pipeline and Orbae data model

First pass, 2026-09-24. One chart for both flows: the Cornerstone repo as it runs today
(green) and the Orbae emissions data model as proposed (blue). They share everything up to
and including `emit`; the two amber boxes below `emit` are the **alignment point still to
be clarified** — what `emit` produces today versus what the data model asks it to produce.

Drawn from [`data_flow.md`](data_flow.md), [`orbae_emissions_data_model.md`](orbae_emissions_data_model.md),
[`jdluc_derivability.md`](jdluc_derivability.md), and [`orbae_spec_alignment.md`](orbae_spec_alignment.md) §0.

```mermaid
%%{init: {"layout": "elk", "elk": {"considerModelOrder": "NODES_AND_EDGES", "nodePlacementStrategy": "LINEAR_SEGMENTS"}}}%%
flowchart TB

  %% Layout: ELK with model order, so declaration order drives left-to-right placement.
  %% Only three subgraphs are kept (Cornerstone repo, alignment point, outputs): every
  %% subgraph is a rigid box ELK has to place as a unit, and with more of them the Orbae
  %% lane drifted off centre. Other groupings are shown by colour only. The crop side is declared first
  %% (left), the Cornerstone downstream last (right). Cornerstone downstream is fed from
  %% emit rather than the "Repo today" box so it sits level with the alignment point: a
  %% node always lands one layer below whatever feeds it. Viewers without the ELK plugin
  %% fall back to dagre; the chart still renders, just with a looser layout.
  %% Detail lines are the italic lines inside labels; tools/strip-mermaid-detail.py
  %% removes them to produce the headings-only version.

  %% ───────────── Cornerstone inputs ─────────────
  src_stocks["`**Carbon stocks**
  AGB · BGB · SOC · peat`"]
  src_events["`**LUC event dates**
  TCL · GPW natural grassland · GPW pasture`"]
  src_climate["`**Climate zone**`"]
  src_tables["`**Constants + lookup tables**
  peat emissions · grassland emissions`"]

  %% ───────────── crop side (left, de-emphasised) ─────────────
  src_crop["`Crop layer
  crop-specific or country-specific`"]
  src_yield["`Yield`"]
  cropprep["`Crop-specific pre-processing`"]
  croplayer["`Crop layer`"]
  src_crop --> cropprep
  src_yield --> cropprep
  cropprep --> croplayer

  %% ───────────── Cornerstone pipeline ─────────────
  subgraph cornerstone["Cornerstone repo"]
    ingest["`**ingest**`"]
    harmonize["`**harmonize**
    *57 bands · 30 m · 10° tiles*`"]
    emit["`**emit**
    *conversion record + per-pixel emissions*`"]
    ingest --> harmonize --> emit
  end
  src_stocks --> ingest
  src_events --> ingest
  src_climate --> ingest
  src_tables --> ingest

  %% ───────────── alignment point ─────────────
  subgraph align["⚠ ALIGNMENT POINT — to be clarified"]
    emitout["`**Repo today: emit zarr**
    *20 bands · 5-yr spans · destination required*
    *reference year baked in*`"]
    L1["`**Orbae: Layer 1 — non-crop-specific emissions layer**
    *one global layer per reference year*
    *undiscounted stocks · no crop info · no destination gate*`"]
  end
  emit --> emitout
  emit -. proposed .-> L1

  %% ───────────── Orbae ─────────────
  L2["`**Crop-specific derived emissions data**
  *amortised · crop-specific FLU and peat factors*
  *potential emissions if the crop occupies the pixel*`"]
  newjd["`**per-pixel jdLUC input data**
  *crop and baseline gates applied*`"]
  annual["`**Annual Data Series**
  *split by conversion year*`"]
  jdagg["`**jdLUC aggregation**
  *Σ over ADM3 · extensive values only*`"]
  statalloc["`**Statistical allocation**
  *ADM3 share × ADM3 totals*`"]
  L1 --> L2
  croplayer --> L2
  croplayer --> newjd
  L2 --> newjd
  L2 --> annual
  newjd --> jdagg
  L2 --> statalloc

  %% ───────────── Cornerstone downstream (right, de-emphasised) ─────────────
  src_mapspam["`MapSPAM`"]
  downscale["`downscale
  *MapSPAM ~10 km grid*`"]
  attribute["`attribute
  *admin × crop*`"]
  trace["`trace
  *EF in kgCO₂e / kg*`"]
  emit -->|statistical leg| downscale
  emit -->|direct leg| attribute
  src_mapspam --> downscale
  downscale --> attribute --> trace

  %% ───────────── outputs ─────────────
  subgraph outputs["Outputs"]
    legacyjd["`**legacy jdLUC**
    *ADM roll-up · ratios · % · per kg*`"]
    orbae_app["`**Orbae**`"]
    whatif["`**What If**`"]
    L3["`**Emissions Layer**
    *total tCO₂e/ha · ha/px · crop_present*`"]
    proxy["`**jdLUC Proxy**`"]
    dluc["`**dLUC**`"]
  end
  csjd["`**Cornerstone jdLUC**`"]
  jdagg --> legacyjd
  jdagg --> orbae_app
  annual --> orbae_app
  orbae_app --> whatif
  L2 --> L3
  statalloc --> jdagg
  jdagg --> proxy
  L2 --> dluc
  trace --> csjd

  %% ───────────── other sources ─────────────
  src_admin["`**Admin boundaries**`"]
  src_cropgrids["`**CropGrids**`"]
  src_client["`**Client data**
  farm polygons · supply sheds · yield`"]
  src_admin --> jdagg
  src_cropgrids --> statalloc
  src_client --> dluc

  %% ───────────── styling ─────────────
  classDef cs fill:#e8f5e9,stroke:#2e7d32,color:#000;
  classDef dim fill:#fafafa,stroke:#9e9e9e,color:#616161;
  classDef dimsrc fill:#fafafa,stroke:#bdbdbd,stroke-dasharray:4 3,color:#757575;
  classDef ob fill:#e3f2fd,stroke:#1565c0,color:#000;
  classDef out fill:#1565c0,stroke:#0d3c78,stroke-width:2px,color:#fff;
  classDef alignBox fill:#fff3cd,stroke:#e0a800,stroke-width:3px,color:#000;
  classDef src fill:#f5f5f5,stroke:#757575,stroke-dasharray:4 3,color:#000;
  class ingest,harmonize,emit cs;
  class downscale,attribute,trace,src_mapspam,cropprep,croplayer dim;
  class src_crop,src_yield dimsrc;
  class L2,newjd,annual,jdagg,statalloc ob;
  class L3,dluc,legacyjd,proxy,orbae_app,whatif,csjd out;
  class emitout,L1 alignBox;
  class src_stocks,src_events,src_climate,src_tables,src_admin,src_cropgrids,src_client src;
  style cornerstone fill:#f1f8f1,stroke:#2e7d32,stroke-width:2px;
  style outputs fill:#f3f8fd,stroke:#0d3c78,stroke-width:2px;
  style align fill:none,stroke:#e0a800,stroke-width:2px,stroke-dasharray:6 4;
```

Green: Cornerstone repo stages. Light blue: Orbae data model intermediates. Dark blue:
outputs. Grey dashed: external data sources. Amber: the two candidate outputs of `emit`, side by side.

Italic lines inside boxes are detail. For a headings-only version, run
`tools/strip-mermaid-detail.py docs/orbae/overall_data_flow.md` and render the result.

## Notes

- **Alignment point.** "Repo today: emit zarr" is what `emit` writes at `8130655`
  ([data_flow.md](data_flow.md#emit--20-bands)). Layer 1 is what the data model pitches as
  "the output of the emit step". The dotted edge is the proposal, not the state of the code.
- **Cornerstone downstream.** Drawn from `emit` rather than from the "Repo today" box so it
  sits level with the alignment point; it consumes the same data. The direct leg skips
  `downscale`; the statistical leg goes through it onto the MapSPAM grid.
- **Orbae lane.** Crop-specific derived emissions data holds values for every pixel; the crop
  and baseline gates are applied in the per-pixel jdLUC input data (derivability §5,
  decision 1.8), and jdLUC aggregation sums it over admin boundaries. How dLUC is produced
  from it and client data is not yet specified.
- **Not drawn.** Forecasting, backcasting, palm, the `validation/` package, and the spec's
  step-5 filter layer.
