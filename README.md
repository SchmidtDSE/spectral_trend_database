#  SPECTRAL TREND DATABASE

DSE’s Spectral Trends Database monitors uses data from NASA's Landsat satellites to track over 14,000 points in corn and soy fields in the midwestern United States. The database contains daily values for 36 different vegetation indices from the year 2000 to present, along with a number of derivative metrics that are useful for detecting crop planting and harvesting. The data will be useful for myriad agriculture applications, including the study and monitoring of yield, yield-stability, soil
health, cover-cropping, and other sustainable agricultural practices.


- [Project Description](https://schmidtdse.github.io/spectral_trend_database)
- [API Documentation](https://schmidtdse.github.io/spectral_trend_database/docs)
- [DSE](https://dse.berkeley.edu)

---

## DATABASE DESCRIPTION

<!-- start_db_description -->
The database consists of 10 tables described below. Depending on the table they may be joined on `sample_id`, or if there is a time component, `sample_id + year`.  Additionally, the several of the datasets contain list-valued cells containing data for specific dates. This in indicated below with "[date]" and "[(daily) date]". Note this structure will probably be dropped in future versions.

Here is a quick overview of the tables:

* `SAMPLE_POINTS` (sample_id): location information such as lat, lon and geohashes. Note that the sample_id itself is a 11-character geohash.
* `ADMINISTRATIVE_BOUNDARIES` (sample_id): administrative information such as state and county.
* `QDANN_YIELD` (sample_id, year): yield estimations for year
* `LANDSAT_RAW_MASKED` (sample_id, year, [date]): landsat band values for year
* `RAW_INDICES_V1` (sample_id, year, [date]): spectral indices built from `LANDSAT_RAW_MASKED`. For a list of indices and how there are calculated see [config/spectral_indices/v1](https://github.com/SchmidtDSE/spectral_trend_database/blob/main/config/spectral_indices/v1.yaml)
* `SMOOTHED_INDICES_V1` (sample_id, year, [(daily) date]):
interpolated and smoothed daily values for indices contained in `RAW_INDICES_V1`
* `MACD_INDICES_V1` (sample_id, year, [(daily) date]): additional indices dervived from `SMOOTHED_INDICES_V1` whose values are useful for detecting cover-croping and green-up dates.
* `INDICES_STATS_V1` (sample_id, year, [date]): statistical (min, max, mean, median, skew, kurtosis) aggregation of `SMOOTHED_INDICES_V1`
* `INDICES_STATS_V1_GROWING_SEASON` (sample_id, year, [date]): same as `INDICES_STATS_V1` but restricted to the "growing season"
* `INDICES_STATS_V1_OFF_SEASON` (sample_id, year, [date]): same as `INDICES_STATS_V1` but restricted to the "off season"

<!-- end_db_description -->


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


