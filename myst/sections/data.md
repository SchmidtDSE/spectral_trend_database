(samples)=
# Sample Selection

```{figure} ../../assets/cornsoy-reduction
:label: im_cld_reduction
:alt: figure: neighborhood reduction of CDL corn/soy
:width: 100%
:align: center

Masking border values in CDL. Left: corn/soy and other, center: 60-meter radius neighborhood reduction, right: masked borders
```

USDA's [_Crop Land Data Layer_](https://developers.google.com/earth-engine/datasets/catalog/USDA_NASS_CDL) was used to create a set of corn/soy sample points, i.e. points CDL labels as (in most cases alternating between) corn or soy for at least 15 years from 2000 to 2020. In order ensure we had "pure" pixels away from confounding effects of boarders and infrastructure 60-meter raidus neighborhood reductions and only kept pixel values that remained unchanged (see [](#im_cld_reduction)). From the resulting image we selecting an initial 20,000 corn/soy points.



We then used these sample points to extract yield values based on [QDANN](https://gee-community-catalog.org/projects/qdann/) (2008-2022).

