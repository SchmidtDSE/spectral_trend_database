#  SPECTRAL TREND DATABASE

DSE’s Spectral Trends Database monitors uses data from NASA's Landsat satellites to track over 14,000 points in corn and soy fields in the midwestern United States. The database contains daily values for 36 different vegetation indices from the year 2000 to present, along with a number of derivative metrics that are useful for detecting crop planting and harvesting. The data will be useful for myriad agriculture applications, including the study and monitoring of yield, yield-stability, soil
health, cover-cropping, and other sustainable agricultural practices.


- [Project Description](https://schmidtdse.github.io/spectral_trend_database)
- [API Documentation](https://schmidtdse.github.io/spectral_trend_database/docs)
- [DSE](https://dse.berkeley.edu)

---

## DATABASE DESCRIPTION


> CHANGE NOTICE: We are still working on operationalization. The database currently only goes through 2001

The _Spectral Trend Database_ lives on [Google Big Query](https://cloud.google.com/bigquery/docs) and can be accessed directly using big query.  However, we've built a number of python tools to make accessing the data eaiser ([docs](XXX), [example](XXX)).


| Table | Keys | Dates | Daily | Description |
| ---: | :----: | :----: | :----: | :---- |
|  [SAMPLE_POINTS](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#sample_points) | sample_id | False | False | location information such as lat, lon and geohashes |
|  [ADMINISTRATIVE_BOUNDARIES](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#admin_boundaries) | sample_id | False | False | administrative information such as state and county |
|  [QDANN_YIELD](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#qdann_yield) | sample_id, year | True | False | yield estimations for year |
|  [LANDSAT_RAW_MASKED](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#masked_landsat) | sample_id, year | True | False | masked landsat band values for year |
|  [RAW_INDICES_V1](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#raw_indices) | sample_id, year | True | False | spectral indices built from `LANDSAT_RAW_MASKED`|
|  [SMOOTHED_INDICES_V1](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#indices) | sample_id, year | True | True | interpolated and smoothed daily values for indices contained in `RAW_INDICES_V1` |
|  [MACD_INDICES_V1](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#macd) | sample_id, year | True | True |  additional indices dervived from `SMOOTHED_INDICES_V1` whose values are useful for detecting cover-croping and green-up dates |
|  [INDICES_STATS_V1](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#indices_stats) | sample_id, year | True | False | statistical (min, max, mean, median, skew, kurtosis) aggregation of `SMOOTHED_INDICES_V1` |
|  [INDICES_STATS_V1_GROWING_SEASON](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#indices_stats) | sample_id, year | True | False | same as `INDICES_STATS_V1` but restricted to the "growing season" |
|  [INDICES_STATS_V1_OFF_SEASON](https://schmidtdse.github.io/spectral_trend_database/docs/pages/database.html/#indices_stats) | sample_id, year | True | False | same as `INDICES_STATS_V1` but restricted to the "off season"|

---

## QUICK SETUP

<!-- start_setup -->
1. Clone Repo

```bash
git clone https://github.com/SchmidtDSE/spectral_trend_database.git
```
2. Install Requirements

```bash
cd spectral_trend_database
mamba env create -f conda-env.yaml
conda activate stdb
```

3. Install `spectral_trend_database` module

```bash
pip install -e .
```
<!-- end_setup -->

---

## REQUIREMENTS

Requirements are managed through a conda yaml [file](./conda-env.yaml). To create/update the `ENV_NAME` environment:

```bash
# create
mamba env create -f conda-env.yaml

# update
mamba env update -f conda-env.yaml --prune
```

---

## USAGE & DOCUMENTATION

See [API Documentation](https://schmidtdse.github.io/spectral_trend_database/docs)
and accompanying [notebooks](https://github.com/SchmidtDSE/spectral_trend_database/tree/feat/apidocs/nb/public)
for detailed examples on how access the database and use the `spectral_trend_database` module.

--- 

## STYLE-GUIDE

Following PEP8. See [setup.cfg](./setup.cfg) for exceptions. Keeping honest with `pycodestyle .`


