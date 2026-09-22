# **Orbae Emissions Layer — Draft Specification**


THIS DOCUMENT IS AN OBJECT OF DISCUSSION. 
IT DOES NOT REPRESENT THE CURRENT STATE OF THE REPOSITORY. 
IT IS HERE FOR COMPARISON PURPOSES ONLY. 
DO NOT TREAT AS A REFERENCE. 
Live version: https://docs.google.com/document/d/1OIevmee4GZqhuvW7mtaUxCQghBElAqvcBtFA4ucKoKY/edit?tab=t.0

## The emissions layer encode the conversion and corresponding emissions associated with the 

## 

## **1 · Key Principles**

The development of the emissions layer is bounded by the following key principles:

* **Preserve the conversion evidence base.** Keep the detection and recording of conversions separate from decisions about which conversions are relevant to a particular downstream use.  
* **Unfiltered first.** Every land conversion we detect goes into the layer, no matter what the land became afterwards. Filtering and cleaning happen later, as separate and documented steps applied on top of the finished layer — never while the layer is being built. (Cornerstone's proof of concept, which only kept conversions ending in cropland and buildup, etc., is the pattern to avoid.)  
* **No crop information inside the layer.** Crop masks, yields and farm polygons stay outside. Because the layer does not know which crop grows where, it carries the soil-carbon result for all three crop groups (annual, perennial, rice, pasture, forestry) side by side, and leaves peat occupation without a detected conversion, as well as the crop attribution itself, to the downstream step.  
* **Every intermediate result is a real, named product.** The layer is built in a handful of steps, and each step leaves behind a stored result with a fixed set of columns and a check that it passed. Each step works only from the result of the step before it.  
* **Sequence the processing in a way that simplifies computation, i.e.,**   
  * **work on total emissions as long as possible \- in order to simplify aggregation.**   
  * **No reference year until the very end.** Where and when land was converted, and how much carbon that was released, are computed without any reference year, assessment window or amortization rule. Those are applied as a final, cheap step — so one computation serves every year and every amortization scheme.  
* **Avoid cleaning rules that create artificial changes in the results for different reference years.** The consistent use of  TCL and GPW (cascading use, removal of flapping pixels, etc.) in a way that does not make LUC GHG results dependent on the rules, i.e., minimize the influence of these rules on the reference year. 

## **2 · Requirements**

* **No filtering of conversion events with crop masks.** The core layer must retain every event produced by the approved conversion-detection process, irrespective of destination land use. Application-specific exclusions must be applied only in separately identified downstream products.  
* **Scenario-specific results per crop type:** The core layer must provide scenario-specific results for every supported land-use /crop-type category–annual, perennial, paddy rice, pastureland, and forestry– without assigning a category to a location using crop masks, yields, or farm polygons. Spatial attribution must occur downstream  
* **Emissions breakdown/resolution:** Emissions must be computed by ecosystem converted (forest, natural grassland, pasture), by carbon pool (biomass, mineral soil, peat) and by gas (CO₂, CH₄, N₂O). The open version is an aggregation of the full version.  
* **Peat:** The core layer must embed emissions from peat occupation to all peat pixels \- whether converted or not. On top of these emissions from occupation we assign emissions from conversion if we have a converting pixel. The downstream delivered product combines the two while retaining their separate contributions.  
* **The core layer is derived from a consistent conversion-event period**. For fixed source-data versions and processing rules, changing the assessment reference year must not change the underlying conversion-event records or their unamortized emissions. Reference-year selection, assessment-window eligibility, and amortization must be applied downstream.  
* **Perhaps too much implementation focus….**The core must retain unamortized event-level emissions by ecosystem converted, carbon pool, gas, and supported land-use scenario. Aggregated totals must be derived from those components.


  
**OPEN Tasks & Questions**

* **Expand the emissions layer to pasture and forestry.**  In order to facilitate use of the emissions layer for forestry and livestock, we need to compute SOC losses specifically towards pastureland but also towards forest conversion \- need to add the LUCfactor defaults to the climate zone shape file and then compute emissions specifically for these two additional use cases **(overall we have an emissions layer for annual, perennial (tree crops), paddy rice, pasture and (potentially) forestry.**   
* **How the rules for GPW affect its use and the application of linear depreciation**. If we have several conversion events recorded for the same pixel, i.e., a conversion from natural grassland in 2006 and in 2016 and in 2023, how should we handle this?   
  * If we work with last event, we would select 2022  
* **Consistent breakdown of CO2, N2O and CH4**:   
  * **Peat:**   
    * Currently: the cornerstone pulse with an average of method 2.3   
    * Better: Develop a more refined breakdown specifically for cornerstone  
  * **Biomass and SOC:**   
    * Currently: general breakdown based on method 2.3 for both SOC and biomass \- continental WRI approach  
    * Better \-\> Biomass: We can compute with GFW fire database (if we work with GFW Forest gross emissions this breakdown for biomass is already included \- hurray)  
    * Better \-\> SOC: compute N2O emissions from dinitrification \- this results from SOC losses \-\>  \- see EQUATION 11.8 \- FSOM. & table 11.3 here: [https://www.ipcc-nggip.iges.or.jp/public/2006gl/pdf/4\_Volume4/V4\_11\_Ch11\_N2O\&CO2.pdf](https://www.ipcc-nggip.iges.or.jp/public/2006gl/pdf/4_Volume4/V4_11_Ch11_N2O&CO2.pdf) “mineralisation of N associated with loss of soil C in mineral and drained/managed organic soils through land-use change or management practices,”  
* **One result, several delivery formats.** The layer is computed once in a particular format (pixel or h3 cells); the cloud-optimized GeoTIFF, Zarr and hexagon-table deliverables are all exports of that same result. Exports only re-grid and subset — they never recompute.  
* 

## **3 · Calculation sequence**

Eight steps. Each one takes named inputs, produces a named intermediate result, and hands it on. Every intermediate result is written out and kept. Nothing is dropped anywhere in steps 1 to 4 — removal only ever happens by applying a filter from step 5, and that is always a separate decision.

### **Step 1a — Event sources**

**What it does.** Establishes where and when something could have happened. This is an observation layer only: it records candidates, it does not judge or filter them.

**Inputs.**

*Tree cover loss.* Global Forest Watch tree cover loss (Hansen, GFC-2025-v1.13), which gives the year of loss per pixel for 2001 to 2025\. A pixel is treated as forest only where tree canopy cover in the year 2000 exceeds ten per cent. Pixels that show a loss but sit below that canopy threshold are not deleted — they are kept and marked, so the effect of the threshold stays visible.

Alternative: Forest gross emissions layer: 

*Grassland and pasture.* Global Pasture Watch, version 2, annual, 2000 to 2024\. We use two classes: pasture, and natural grassland — into which the shrubland and rangeland category is merged. Two events are read from the annual series: the year a pixel leaves natural grassland (to anything else, pasture included), and the year a pixel leaves pasture. Where a pixel leaves the same class more than once across the series, only one conversion is relevant, and it is **the last one**. The first occurrence is retained alongside as an audit attribute, because the peat pulse in step 2.2 needs it.

*Peat.* Peatland presence from Xu et al. (2018), PEATMAP. Peat is not an event source and does not compete with the other two. It is a property of the soil, it needs no year, and on its own it is already enough to make a pixel emit — see step 2.2. (The Global Forest Watch peat composite is a second-version input, not this one.)

**Output.** Per pixel: the forest loss year, the natural-grassland loss year, the pasture loss year, the number of class changes observed, a flag for pixels that leave the same class more than once, a flag for loss below the canopy threshold, and peat presence. Unfiltered.

### **Step 1b — Value sources**

**What it does.** Assembles the numbers that the emissions will later be based on. Nothing is attached to any pixel here — that happens in step 3\. This step exists so that every default has one declared origin and one version stamp.

**Inputs.**

*Forest Biomass.* Above-ground biomass for the year 2000 (Harris et al.). Below-ground biomass (Huang et al.); where it is absent, below-ground biomass is taken as twenty-five per cent of above-ground biomass, the same fallback the Cornerstone proof of concept uses.

*Soil carbon.* Soil organic carbon stock for the top thirty centimetres, from SoilGrids (ISRIC). Expected range after scaling: roughly fifty to two hundred tonnes of carbon per hectare. The scale factor stored with the layer must be verified against that range before production — the documentation annotation we have is wrong.

*Climate zone with biomass default values for pasture, peat drainage and SOC loss.* The AdAstra climate-zone map (FAO Global Ecological Zones). It carries, per zone: the soil-carbon loss factors by crop group; the natural-grassland and pastureland biomass stocks; and peat drainage rates.

*Woody top-up.* Country-level woody biomass carbon (CTrees), added to the climate-zone pastureland stock. It is not in the climate-zone map and is joined by country.

*Dead organic matter.* Dead wood and litter as fractions of above-ground biomass, by climate zone, following the Cornerstone proof of concept (CDM AR-TOOL-12).

*Peat emission factors.* Emission factors per climate zone and land use, with the split into carbon dioxide, methane and nitrous oxide, from Orbae Methodology v2.3 (IPCC 2013 Wetlands Supplement, Tier 1).

*Gas shares.* Regional contribution shares of carbon dioxide, methane and nitrous oxide in land-conversion emissions, from Fitts et al. (2025).

*Pixel area.* Computed from latitude, not assumed constant.

**Output.** A versioned set of value layers on the same thirty-metre grid, resampled by nearest neighbour, each carrying its source and version.

### **Step 2.1 — Conversion event**

**What it does.** Reduces the candidates from step 1a to exactly one conversion event per pixel.

**How it decides.** By materiality, not by date. Forest outranks natural grassland, which outranks pasture. A forest loss in 2020 therefore wins over a grassland loss in 2008 on the same pixel. Likewise, a natural grassland loss in 2005 therefore wins over a pastureland loss in 2012\. Within Global Pasture Watch, the last conversion out of the class is the event.

No destination class is read at this point. What the land became is not part of the event.

**Output.** Per pixel: which ecosystem was converted, the conversion year, how many candidates the pixel had, and whether the cascade had to be applied. The candidate count and the cascade flag stay in the layer so that every choice can be audited afterwards.

### **Step 2.2 — Peat event and soil regime**

**What it does.** Sets the soil regime and classifies peat pixels into the two peat cases.

*Soil regime.* Organic where peat is present, mineral everywhere else. This is a strict either-or: it decides which soil emissions step 3 computes, and the two are never both computed on the same pixel.

*Peat with a conversion event* becomes a peat conversion. It is dated by the **first** conversion year observed on the pixel — not by the step 2.1 event year. The drainage pulse fires once per pixel, ever.

*Peat without a conversion event* becomes peat occupation. It has no year and is treated as ongoing; the year is assigned in step 4\. This works because all peat which can be occupied by a crop (and that includes both, peat with and peat without a conversion event) emits in the refined peat model.

No land cover data is consulted in this step. Whether occupied peat is in fact drained is a land-cover question, and therefore a detached filter in step 5, never part of creation.

**Output.** Soil regime, peat conversion year, the first conversion year, a flag marking pixels where the pulse year differs from the step 2.1 event year, and the peat occupation mask.

### **Step 3 — Emissions per event**

**What it does.** Converts each event into emissions, undiscounted, per pixel, per carbon pool and per gas. This is where the value sources from step 1b are attached.

**Carbon pools.**

*Biomass.* For forest: above-ground plus below-ground biomass carbon on the pixel. For natural grassland: the climate-zone natural-grassland stock plus the country woody top-up. For pasture: the climate-zone pastureland stock.

*Dead organic matter.* Forest only. Dead wood and litter as climate-zone fractions of above-ground biomass.

*Mineral soil organic carbon.* The soil carbon stock multiplied by the climate-zone loss factor. Because the destination is not known, this is computed in three parallel variants — annual crops, perennial crops, rice, pasture, forestry — which are carried side by side and never collapsed. Mineral soil only.

*Peat conversion pulse.* 621 tonnes of carbon dioxide equivalent per hectare, taken from the Cornerstone proof of concept. Organic soil only, and only where a conversion event exists.

*Peat occupation.* 37.3 tonnes of carbon dioxide equivalent per hectare per year, flat, taken from the Cornerstone proof of concept, as an annual ongoing flow. It applies to **every** organic-soil pixel, whether or not a conversion was observed there. It carries no climate-zone or land-use dimension, because Cornerstone's value has none.

The two peat terms are additive, not alternatives. A converted peat pixel carries both: the occupation flux because the soil is drained, and the pulse because the drainage happened. This is also how the Cornerstone proof of concept combines them — its final sum is the discounted conversion total, which contains the pulse, plus an occupation term computed over all peatland independently of any event.

Put the pool rules together and a converted forest pixel on peat carries four terms: biomass, dead organic matter, the peat transformation pulse and peat occupation pulse. It never carries mineral soil organic carbon, because the soil regime is organic. The same pixel on mineral soil carries three: biomass, dead organic matter and mineral soil carbon. And an unconverted peat pixel carries exactly one: occupation. The soil regime switches between the mineral term and the peat terms; it does not touch biomass or dead organic matter, which are decided by the converted ecosystem alone.

**Gases.**

For the non-peat pools, the carbon released is expanded into the three gases using the regional shares of Fitts et al. (2025). Methane is partitioned out of the released carbon, so the carbon mass balance closes exactly; nitrous oxide carries no carbon and is added on top. In South America this makes one tonne of released carbon 3.708 tonnes of carbon dioxide equivalent, against 3.667 if it were all carbon dioxide.

For the peat pools, the totals are Cornerstone's and only the composition is ours.

*Occupation* is split using Cornerstone's own published components of its 37.3: 29.0 tonnes of carbon dioxide on site plus 1.1 tonnes of carbon dioxide from dissolved organic carbon, 1.6 tonnes of carbon dioxide as methane and 5.6 tonnes of carbon dioxide as nitrous oxide. Those component values are stated on AR6. We keep the physical gas masses — 30.1 tonnes of carbon dioxide, 59.3 kilograms of methane, 20.5 kilograms of nitrous oxide per hectare per year — and re-express them on AR5, which is the only self-consistent way to carry them into our accounting. The carbon dioxide equivalent total therefore lands at 37.20 rather than 37.3. The masses are physical; the equivalent total is a function of the chosen global warming potentials, so the masses are what we preserve.

*The drainage pulse* has no published gas split, and none can be recovered from the 621 itself — it is a least-squares fit of the Greenhouse Gas Protocol ramp to an all-gas curve, not a sum of gas terms. We therefore take the composition from Table 6 and apply it to Cornerstone's total. The shares are averaged across the land uses of the pixel's climate zone, weighted by magnitude, so that each land use counts in proportion to how much peat emission it actually represents. For the tropical zone that gives 94.13 per cent carbon dioxide, 2.52%methane and 3.36%nitrous oxide, which turns 621 into 584.5 tonnes of carbon dioxide, 557.7 kilograms of methane and 78.7 kilograms of nitrous oxide per hectare. Temperate and boreal zones are markedly less carbon-dioxide-dominated, at 80.1% and 77.6%

Two things about this construction should stay visible. The averaging is weighted by magnitude rather than taken as an unweighted mean of the per-land-use shares, because the unweighted mean lets a very small row distort the result — boreal perennial cropland emits 1.88 tonnes of carbon dioxide equivalent per hectare per year but is 22 per cent methane, and under equal weighting it would drag the boreal methane share from 6.6 to 11.6 per cent. And Table 6 describes the annual drainage flux, not the one-off loss from the peat profile; the pulse is most likely more carbon-dioxide-dominated than these shares suggest. This is a defensible proxy, not a derivation, and it is declared as such.

**Open item**. This is a placeholder and needs refinement

**Global warming potentials.** AR5, hundred-year: methane 28, nitrous oxide 265, per Orbae Methodology v2.3.

**Output.** One layer per pool and gas, plus totals, in tonnes per pixel, undiscounted.

### **Step 4 — Reference year**

**What it does.** Places the emissions in the reference year by amortising them over a twenty-year window.

**How.** Years since conversion is the reference year minus the conversion year. A pixel is in the window if that difference is between zero and nineteen.

Three amortisation variants are computed and kept side by side, so the choice stays an output setting and not a baked-in assumption:

*Linear.* The weight is twenty minus the years since conversion, divided by 210\. Recent conversions weigh most, and the weights sum to exactly one across the twenty years — no carbon is created or destroyed.

*Equal.* One twentieth in each of the twenty years.

*None.* The full amount in the year of the event, nothing in the others.

Events older than the window get a weight of zero. They stay in the layer, marked, rather than being removed.

The peat conversion pulse is amortised like every other pulse — timing matters for it. Peat occupation is not amortised: it is an annual flow, and it enters the reference year at its full annual value. The reference year is the only place where both peat terms are in the same units, so it is also where their sum is written out: occupation on all peat, plus the amortised pulse on the converted subset.

**Output.** Per pixel: the amortisation factor, years since conversion, the in-window flag, and the emissions in the reference year per pool and per gas, for each of the three amortisation variants and each of the three soil-crop variants.

### **Step 5 — Filters**

**What it does.** Nothing, by itself. Each filter is a separate boolean layer, computed but never applied during creation. Applying them is a downstream choice, made explicitly and reversibly.

The filters currently defined: loss on pixels below the ten per cent canopy threshold; and pixels that leave the same class more than once.

**Output.** One boolean layer per filter, plus the count of pixels each one would remove.

### **Step 6 — Export**

**What it does.** Writes the layer out in long form, one row per pixel: coordinates, converted ecosystem, conversion year, soil regime, the three peat masks (occupation, conversion, no event), every pool-by-gas value for the selected amortisation and crop variant, the combined peat total, every filter flag unapplied, and the version stamp of every input that contributed.

Optional but relevant in the future step: AI agent readiness

AI is becoming more and more part of everyone's life, moving the interface of human with data away from the tech closer to the human with natural language. It is important AI agents can easily interface with our data leaving minimal space for erroneous interpretation from the agent.

Raster data, according to Claude, is more difficult to interpret by agents than tabular data (crs and reference point might cause the agent to be shifted by some pixels). Also, depending where we set the interface between agent and data, the conversion to tabular data might need to occur earlier or later (e.g. already at “emission database" level where we did not apply the math).

One ambition of an AI use case could be that a client uploads an excel file with locations (LMU, country, general region, jurisdiction), which could be even mixed within the file, assessment years (could also be multiple in the file) and crop name and the agent could automatically put the puzzle pieces together and return a filled out excel template (incl. Breakdowns of ecosystems etc) and maybe even multiplied with the client procurement volumes.

This could well only live in our infrastructure and not be open source, but be our added value that we bring the the open source stack.

---

## **Declared differences to the Cornerstone proof of concept**

Differences in the input data stack are excluded here — Cornerstone will follow our stack (tree cover loss plus Global Pasture Watch), so those are not differences to declare. What follows is everything else that remains open between the two methods.

**Global warming potentials.** We use AR5 (methane 28, nitrous oxide 265), per Orbae Methodology v2.3; Cornerstone uses AR6 (27 and 273). On identical physical flux our methane term is about 3.7 per cent higher and our nitrous oxide term about 2.9 per cent lower.

**Amortisation arithmetic.** We use a discrete linear weight of twenty minus years since conversion over 210, which sums to exactly one across twenty annual steps. Cornerstone uses a continuous form over 200, applied to five-year spans, which sums to 1.05 across the same window. Both are linear declines; only ours conserves mass.

**Climate-zone keys.** We use our own climate-zone map, in which tropical moist and tropical wet are collapsed into one zone. Cornerstone distinguishes tropical wet, moist, dry and montane. Where a mapping is unavoidable — the dead organic matter fractions — we map rainforest to tropical wet and moist forest to tropical moist. Deliberate for now; revisable.

**Peat occupation values.** We use climate-zone and land-use specific factors with a published gas split (Orbae v2.3; tropical annual cropland 56.92 tonnes of carbon dioxide equivalent per hectare per year). Cornerstone uses a single flat 37.3, which is its temperate cropland value. In the tropics that is a difference of roughly plus fifty per cent on the occupation term. The 621 pulse is taken from Cornerstone unchanged.

**Peat pulse gas split.** Cornerstone publishes none, and none can be derived: 621 is a fitted total, not a sum of gas terms. We currently split it at the annual-cropland occupation shares. Placeholder — flagged as an open decision, not a settled difference.

**Gas disaggregation of non-peat emissions.** Cornerstone reports carbon dioxide equivalent only. We report the three gases separately for every pool, using the regional shares of Fitts et al. (2025), with methane partitioned out of the released carbon so the carbon mass balance closes.

**Position of filtering.** Cornerstone filters inside creation — its soil term fires only on transitions that end in cropland. We produce the unfiltered layer first and keep every filter as a detached boolean. This is the single most consequential architectural difference between the two, and the reason our layer can be re-filtered without being recomputed.

**Crop-group handling.** Cornerstone resolves one soil loss factor per pixel. We carry three parallel soil variants — annual, perennial, rice — unresolved through to export, because the layer is crop-agnostic by design.

**Grassland biomass source.** Cornerstone uses Houghton/BLUE grassland carbon. We use our climate-zone natural-grassland stock plus a country woody top-up from CTrees: for the tropical moist and wet zone in Brazil, 32.0 plus 15.93 tonnes of carbon per hectare, or 175.7 tonnes of carbon dioxide per hectare in total.

**Versioning discipline.** Cornerstone's cache keys are content-blind and require a manual version bump when an assumption changes — a silent-staleness risk. We stamp the version of every contributing input into the export itself.

## 

## 

## 

## **3 · Attribute skeleton — what is created where**

| Created at | Attributes | Lifecycle |
| :---- | :---- | :---- |
| **L1** keys | h3\_index · cell\_area\_m2 · country\_iso, admin1–3\_id · climate\_zone · method\_version, input\_versions | Carried unchanged to L5; admin keys and climate zone dropped in COG. |
| **L1** observations | tcl\_loss\_year · tcl\_from\_fire · gpw\_natgrass\_seq, gpw\_pasture\_seq · peat\_present · agb\_tC\_ha, bgb\_tC\_ha · soc\_tC\_ha | Carried to L3; stocks consumed into fluxes at L3; sequences dropped after L3. |
| **L2** events | ecosystem\_converted · conversion\_year · events\_all · peat\_converted | Carried to L6 as ecosystem and year bands; conversion\_year → amortization factor at L4. |
| **L3** fluxes | \<pool\>\_\<variant\>\_\<gas\> — pools {biomass, dom, soc, peat\_pulse, peat\_occ\_rate}; variants {annual, perennial, rice} for soc; gases {co2, ch4, n2o} | Scaled at L4; collapsed to shipped bands at L6. total\_co2e always derived. |
| **L4** reference | reference\_year · window\_years · amortization\_scheme · amortization\_factor · em\_\<pool\>\_\<variant\>\_\<gas\> | Parameters become export metadata; emissions become bands. |
| **L5** filters | filter\_\<name\> · filter\_spec\_version | Become masks / subsets at L6. |

## 

## 

