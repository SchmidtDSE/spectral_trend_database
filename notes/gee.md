# STDB: Google Earth Engine

The majority of the data extraction, preparation and processing for STDB takes occurs using python. This includes the GEE python-api.

However initial data-selection is done within the GEE javascript playground.  Below are details for which scripts are used, the steps taken and outputs produced.

---

## Corn Soy Other


```yaml
script: [CDL_CS_OB_SamplePoints-Exports]https://code.earthengine.google.com/2685255eafc3faedd45df0665362c909
result: projects/dse-regenag/assets/CDL/cornsoy_other-n60-y15-p20000
visulaization script: (wip) https://code.earthengine.google.com/e6c0ee4ec7d3212580d2a9edeaecd3b4
```

The first step is to create a stratified-sample of potential corn/soy/other points.  In fact the processing produces one additional class "border", however neither "other" or "border" are used in STDB.

The "border" class is created so that it can be remove ensuring that the corn/soy points are pure.

For each year ([2000,2020]):

1. Select CDL for year
2. Map CDL values to corn / soy / other to create `im`
3. Remove borders:
	a. reduce neighborhood (60-meters) to create `neighborhood_im`
	b. replace values where `im != neighborhood_im`: `im = neighborhood_im.where(neighborhood_im.neq(im), BORDER_VALUE).toInt()`

From the resulting image collection:

1. Require at least 15 years of CSO value (out of 20) to be continual_im
2. Perform a stratified sample

---

## Yield Exports

```yaml
script: [SampleCropYield-QDANN-Exports] https://code.earthengine.google.com/595bedc5425c99e83fc41de9ebf39c69
outputs: gs://agriculture_monitoring/CDL/lobell/QDANN
```

Using the points selected above we then export yield for each of the years of interest.

1. Select Corn/Soy points as (described above)
2. Map over each point:
	a. filter qdann corn xor soy by point
	b. compute value or neighborhood value of biomass at that point
	c. filter out values with no biomass
	d. flatten results


