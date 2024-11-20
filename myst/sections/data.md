(samples)=
# Sample Selection


:::{note} Corn Soy Other (GEE)
:class: dropdown

- csob_ic <= for each year [2000,2020]:
	1. select cdl for year
	2. map to corn / soy / other
	3. remove borders:
		* reduce neighborhood (90-meters)
		* im = neighborhood_im.where(neighborhood_im.neq(im), BORDER_VALUE).toInt()
		* result im each CSO value is CSO iff its at least 90-meters from a different value otherwise B
	4. require at least 15 years of CSO value (out of 20) to be continual_im
	5. stratified_sample of continual_im C-S-O-B [10k,10k,2k,2k]
- result projects/dse-regenag/assets/CDL/cornsoy_other-n60-y15-p20000
- => 24,000 features (10,000 Corn)

script: https://code.earthengine.google.com/c25c8860c473c39ee301448ca8571a6e
visulaization script: (wip) https://code.earthengine.google.com/e6c0ee4ec7d3212580d2a9edeaecd3b4
asset: `projects/dse-regenag/assets/CDL/cornsoy_other-n60-y15-p20000`
:::


```{figure} ../../assets/cornsoy-reduction
:label: im_cld_reduction
:alt: figure: neighborhood reduction of CDL corn/soy
:width: 100%
:align: center

Masking border values in CDL. Left: corn/soy and other, center: 60-meter radius neighborhood reduction, right: masked borders
```

USDA's [_Crop Land Data Layer_](https://developers.google.com/earth-engine/datasets/catalog/USDA_NASS_CDL) was used to create a set of corn/soy sample points, i.e. points CDL labels as (in most cases alternating between) corn or soy for at least 15 years from 2000 to 2020. In order ensure we had "pure" pixels away from confounding effects of boarders and infrastructure 60-meter raidus neighborhood reductions and only kept pixel values that remained unchanged (see [](#im_cld_reduction)). From the resulting image we selecting an initial 20,000 corn/soy points.


:::{note} YIELD DATA (GEE)
:class: dropdown

1. select cornsoy points from csob (described above)
2. map over each point:
	a. filter qdann corn xor soy by point
	b. compute value or neighborhood value of biomass at that point
	c. filter out values with no biomass
	d. flatten results

script: https://code.earthengine.google.com/595bedc5425c99e83fc41de9ebf39c69
:::

We then used these sample points to extract yield values based on [QDANN](https://gee-community-catalog.org/projects/qdann/) (2008-2022).

