(data)=
# Data Selection and Processing

(samples)=
## Sample Selection

```{figure} ../../assets/cornsoy-reduction
:label: im_cld_reduction
:alt: figure: neighborhood reduction of CDL corn/soy
:width: 100%
:align: center

Masking border values in CDL. Left: corn/soy and other, center: 60-meter radius neighborhood reduction, right: masked borders
```

USDA's [_Crop Land Data Layer_](https://developers.google.com/earth-engine/datasets/catalog/USDA_NASS_CDL) was used to create a set of corn/soy sample points, i.e. points CDL labels as (in most cases alternating between) corn or soy for at least 15 years from 2000 to 2020. In order ensure we had "pure" pixels away from confounding effects of boarders and infrastructure 60-meter radius neighborhood reductions and only kept pixel values that remained unchanged (see [](#im_cld_reduction)). From the resulting image we selecting an initial 20,000 corn/soy points.

We then used these sample points to extract yield values based on [QDANN](https://gee-community-catalog.org/projects/qdann/) (2008-2022).

(processing)=
## Data Processing

Having selected data sample points and extracted yield data, we then built a pipeline (see these [scripts](https://github.com/SchmidtDSE/spectral_trend_database/scripts)) to process the data and create a database ([Google Big Query Dataset](https://cloud.google.com/bigquery/docs)) containing daily-smoothed-values for 36 spectral indices, along with additional indices and annual aggregation statistics.

The resulting database is described in the [docs](/docs/pages/database.html).

The most interesting steps in the data processing are: gap-filling and smoothing, and the computation of moving average convergence divergence (divergence) indices.

(smoothing)=
### Gap Filling and Smoothing

Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.


(macd)=
### Moving Average Convergence Divergence (Divergence)

Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

