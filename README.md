#  SPECTRAL TREND DATABASE

DSE’s Spectral Trends Database monitors uses data from NASA's Landsat satellites to track over 14,000 points in corn and soy fields in the midwestern United States. The database contains daily values for 36 different vegetation indices from the year 2000 to present, along with a number of derivative metrics that are useful for detecting crop planting and harvesting. The data will be useful for myriad agriculture applications, including the study and monitoring of yield, yield-stability, soil
health, cover-cropping, and other sustainable agricultural practices.


- [Project Description](https://schmidtdse.github.io/spectral_trend_database)
- [API Documentation](https://schmidtdse.github.io/spectral_trend_database/docs)
- [DSE](https://dse.berkeley.edu)

---

## DATABASE DESCRIPTION

```{include} docs/pages/database.md
:start-after: <!-- start_db_overview -->
:end-before: <!-- end_db_overview -->
```

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


