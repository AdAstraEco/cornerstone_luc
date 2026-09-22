# **Minimal Emissions Layer Contents**

‘Emissions layer’ should actually be a group of layers:

1) **Non-crop-specific emissions layer**  
   The alignment point with watershed. Calculated globally once per reference year. Our pitch for the output of the ‘emit’ step  
2) **Crop-specific emissions layer**  
   What will look most familiar to us. Derived from (1), combined with crop and yield data. Allows us to generate jdluc and dluc  
3) **Client-deliverable emissions layer**  
   What we sell as ‘emissions layer’. Just the total\!  
   The point is that if a client wants intermediate breakdowns, we can easily add them by pulling in earlier columns. We need to be **able** to provide more, not to do it by default

# 1\) Non-Crop-Specific emissions layer 

**One global layer per reference year**  
This represents the potential emissions from land conversion events, without any consideration of the later use of the land.  
Align on this with Watershed. If their codebase provides (or lets us generate) this, then we have everything we need for the emissions layer, for jdLUC and for dLUC

| Row | Meaning | Values | Justification |
| :---- | :---- | :---- | :---- |
| conversion source | Nature of the most significant land conversion event at this location in the 20-year time window | {forest,natural grassland,pasture} | conversion source, year, and emissions are aligned (i.e. emissions are based on what we converted from, but we don’t need to pull that forward separately) |
| conversion-year | Year of the most significant land conversion event at this location in the 20-year time window | int, YYYY |  |
| mineral-soil-carbon-at-risk-tco2e-per-ha | Co2e emissions associated with the most significant land conversion event. Not amortized, not reduced to account for the destination class | float, tco2e-per-ha, undiscounted aligned with conversion events – 0/null if there is no conversion event present | The reason to postpone discounting is so that watershed can work with 5-year groups to align with mapspam, and to allow us to derive equal allocation laterBroken out because we need to use crop-specific flu factors |
| peat-transformation-emissions-undiscounted-tco2e-per-ha |  |  |  |
| vegetation-emissions-undiscounted-tco2e-per-ha |  |  |  |
| peat-occupation-emissions-per-year | Potential annual emissions from peat occupation when this land is occupied by crop. (will need later adjustment depending on climate zone and crop type) | \[needed? unclear if it is derivable from peat-transformation-stock-per-ha\]. float.  |  |
| climate-zone | enum based on whichever climate zone source we choose | enum | needed to find appropriate flu factors |
| continent | for matching with the table of region-specific gas breakdowns | enum | will be mostly the same value throughout a [tile](http://tile.so).  i.e. heavily redundant, but easier to reason about than trying to add in a vector source |
| hectares-per-pixel | convenience, to simplify summing kg/ha figures over pixels | float |  |

And these don’t affect emissions but is useful to reproduce our current jdLUC and the current cornerstone downstream:

| land use in y-20 | Land use at the beginning of the assessment window, according to what's baked into the Cornerstone pipeline. | bitmask: (natural) forest, natural grassland, pasture,cropland, natural forest, unknown | first 3 should match the conversion events. last 3 don’t affect jdluc emissions but are relevant for narrativeKeeping this as a bit mask gives us an idea of conflicts between the different layers \- Collisions between any two of these datasets are readable as pairs of bits being set. |
| :---- | :---- | :---- | :---- |
| destination-dataset | Land use in the reference year, according to what's baked into the cornerstone pipeline | bitmask 0–7: Descals 1 · GACED30 2 · GPW 4 | Discarded by us in favour of specific crop layers. Keeping it present allows Cornerstone to keep using their general-purpose workflow.It might also be a useful future input into forecasting, backcasting, and jdLUC proxy workflows. |

## Crop-Specific data layer 

| Row | Values | Justification |
| :---- | :---- | :---- |
| crop\_present\_in\_reference\_year | bool |  |
| crop\_present\_in\_baseline\_year | bool | no effect on totals, just attribution. may be absent |
| yield  | float |  |

# 2\) Crop-Specific derived emissions layer 

This materializes the calculations for a specific crop,  based on the above.  
It should support everything required for:

- dluc  
- jdLUC  
- Client deliverable

| Row |  | Values | Notes and Questions |
| :---- | :---- | :---- | :---- |
| conversion source |  | {forest,natural grassland,pasture} | conversion source, year, and emissions are aligned (i.e. emissions are based on what we converted from, but we don’t need to pull that forward separately) |
| \[linear discounting weight\] | based on conversion year \-\> reference year |  |  |
| \[destination\_specific\_peat\_factor\] | lookup based on crop type+ climate zone |  |  |
| \[flu factor\] | lookup based on crop type \+ climate zone |  |  |
| mineral-soil-emissions-amortized-per-ha | mineral-soil-stock-per-ha \* flu\_factor \* linear\_discounting\_weight | discounted and with flu factors applied. get amortization factor from conversion and reference year get flu factor from crop and climate zone  |  |
| peat-soil-transformation-emissions-amortized-per-ha | peat-soil-stock-per-ha \* destination\_specific\_peat\_factor \* linear\_discounting\_weight |  |  |
| vegetation-emissions-amortized-per-ha | vegetation-stock-per-ha \* linear\_discounting\_weight |  |  |
| peat-occupation-emissions-per-year | based on climate zone, crop type, and peat-soil-stock-per-ha |  | as above: unclear if we need separate spatially-explicit data for peat occupation vs transformation |
| total-emissions-amortized-per-ha | float: sum of all the previous |  |  |
| hectares-per-pixel | float |  |  |
|  |  |  |  |

# 3\) Client-deliverable data layer

| Row | Values | Justification |
| :---- | :---- | :---- |
| total\_emissions\_per\_ha | float, tco2e\_per\_ha – the total LUC emissions IF the target crop occupies this land in the reference year. Present whether or not we  |  |
| hectares-per-pixel | float |  |
| crop\_present | bool | based on our national crop layer, if we have one. Allows for supply-shed calculations and for validation of internal data |
| Any intermediate columns, at client request | We can pull in data from any of the intermediate tables, if clients have a use for them |  |

Any limitations?

Is there anything we want for jdluc which we **can't** derive from this?

- **Forecasting:** the current implementation does not work in the current form, we need to think about how to apply our current approach or a new one   
  - The same: one option is a special module that runs forecasting after results have been aggregated to the ADM3 level   
  - Different: forecasting at pixel level?  
- **Backcasting:** the current approach does not work in the current form, need to think about how to apply our current approach or a new one on top, i.e., after results have been aggregated to the ADM3 level   
-   
- 

Are there any crop-specific tweaks which **can't** be kept downstream of it?

- Probability maps \-\> works \-\> just a pre-processing exercise 

