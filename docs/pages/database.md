# Database


> CHANGE NOTICE: We are still working on operationalization. The database currently only goes through 2001

The _Spectral Trend Database_ tracks over 14 thousand points in the mid-western United States from 2000 to present. Below we have a detailed description of each of the tables. The database lives on [Google Big Query](https://cloud.google.com/bigquery/docs) and can be accessed directly using big query.  However, we've built a number of python tools to make accessing the data eaiser ([docs](XXX), [example](XXX)).

---

<a name='database'></a>

<!-- start_db_table -->

| Table | Keys | Dates | Daily | Description |
| ---: | :----: | :----: | :----: | :---- |
|  [SAMPLE_POINTS](#sample_points) | sample_id | False | False | location information such as lat, lon and geohashes |
|  [ADMINISTRATIVE_BOUNDARIES](#admin_boundaries) | sample_id | False | False | administrative information such as state and county |
|  [QDANN_YIELD](#qdann_yield) | sample_id, year | True | False | yield estimations for year |
|  [LANDSAT_RAW_MASKED](#masked_landsat) | sample_id, year | True | False | masked landsat band values for year |
|  [RAW_INDICES_V1](#raw_indices) | sample_id, year | True | False | spectral indices built from `LANDSAT_RAW_MASKED`|
|  [SMOOTHED_INDICES_V1](#indices) | sample_id, year | True | True | interpolated and smoothed daily values for indices contained in `RAW_INDICES_V1` |
|  [MACD_INDICES_V1](#macd) | sample_id, year | True | True |  additional indices dervived from `SMOOTHED_INDICES_V1` whose values are useful for detecting cover-croping and green-up dates |
|  [INDICES_STATS_V1](#indices_stats) | sample_id, year | True | False | statistical (min, max, mean, median, skew, kurtosis) aggregation of `SMOOTHED_INDICES_V1` |
|  [INDICES_STATS_V1_GROWING_SEASON](#indices_stats) | sample_id, year | True | False | same as `INDICES_STATS_V1` but restricted to the "growing season" |
|  [INDICES_STATS_V1_OFF_SEASON](#indices_stats) | sample_id, year | True | False | same as `INDICES_STATS_V1` but restricted to the "off season"|

<!-- end_db_table -->

---

## TABLES

---

<a name='sample_points'></a>
### `SAMPLE_POINTS`


> CHANGE NOTICE: sample_id, and geohash references with be replaced with [h3](https://h3geo.org/) in future versions

This tabel contains locational information for sample-points such as lat, lon and geohashes. A detailed description on of how the locations have been selected can be found [here](/data). Note the columns are redundant, as `sample_id` is just an 11-character [geohash](https://en.wikipedia.org/wiki/Geohash), and determines the lat, lon with sufficient accuracy.


| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
|  `geohash_<n>` (`n` in [5, 7, 9]) | additional geohashes redudant with `sample_id`. originally included for initial parameter searches and will be removed in future versions |
|  lon | longitude |
|  lat | latitude |


---

<a name='admin_boundaries'></a>
### `ADMINISTRATIVE_BOUNDARIES`

Administrative information for each sample-point ([source](https://www.geoplatform.gov/metadata/e33c1816-ea7f-4d24-b6e1-e471635d9770)).


| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `AWATER` | water area (square meters) |
| `ALAND` | land area (square meters)  |
| `LSAD` | legal/statistical area description code for county |
| `STATE_NAME` | state name |
| `STUSPS` | United States Postal Service state abbreviation |
| `NAME` | county name |
| `GEOID` | County identifier; a concatenation of current state Federal Information Processing Series (FIPS) code and county FIPS code |
| `GEOIDFQ` | Fully qualified geographic identifier; a concatenation of census survey summary level information with the GEOID attribute value. The GEOIDFQ attribute is calculated to facilitate joining census spatial data to census survey summary files. |
| `COUNTYFP` | county Federal Information Processing Series (FIPS) code |
| `NAMELSAD` | name and the translated legal/statistical area description for county |
| `COUNTYNS` | county Geographic Names Information System (GNIS) code |
| `STATEFP` | state Federal Information Processing Series (FIPS) code |

---

<a name='qdann_yield'></a>
### `QDANN_YIELD`

Modeled yield data from 200X - Present using [QDANN](https://gee-community-catalog.org/projects/qdann/).


| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to submeter precision |
| `year` | year of yield |
| `crop_type` | type of crop ("corn" or "soy" ) |
| `biomass` | modeled biomass yield |

---

<a name='masked_landsat'></a>
### `LANDSAT_RAW_MASKED`


> CHANGE NOTICE: In upcoming versions of STDB we will likely replace list-valued date/band row for each year, with independent rows for each day

Masked Landsat (optical) band values. The band value columns contain lists for each date where data is available.  There is a corresponding `date` column containing a list of dates for each value. The date values range from Sept 1. of the prior year, through December 1 of the year listed to capture dates before and after the dates of interest, namely the off-season and growing-season before yield.

Note: to simplfy analysis STDB offers [tools](/docs/spectral_trend_database/spectral_trend_database.utils.html#spectral_trend_database.utils.row_to_xr) and [examples](XXX) of how to convert each row to an `xr.Dataset`.

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | agricultural year (from Sept 1. of the prior year, through December 1 of the listed year) |
| `date` | list of dates for each value |
| `<band_name>` | cloud-masked landsat values (blue, green, red, nir, swir1, swir2) |


---

<a name='raw_indices'></a>
### `RAW_INDICES_V1`

Spectral indices derived using the band-values in `LANDSAT_RAW_MASKED`. See [config/spectral_indices/v1](https://github.com/SchmidtDSE/spectral_trend_database/blob/main/config/spectral_indices/v1.yaml) for a list of indices and how they are calculated.

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | agricultural year (from Sept 1. of the prior year, through December 1 of the listed year) |
| `date` | list of dates for each value |
| `<index>` | one of: ndvi, ndbr, ndmi, ndwi, msi, rdi, srmir, slavi, wdrvi, bwdrvi, savi, gsavi, mnli, tdvi, evi, evi2, evi22, atsavi, afri1600, cm, cig, gndvi, msavi, gvi, wet, tvi, osavi, rdvi, rvi, grvi, si, si1, gari, gli, msr, nli (see [config/spectral_indices/v1](https://github.com/SchmidtDSE/spectral_trend_database/blob/main/config/spectral_indices/v1.yaml) for details) |



---

<a name='indices'></a>
### `SMOOTHED_INDICES_V1`

Smoothed daily values computed from `RAW_INDICES_V1`. Smoothing computed [here](https://github.com/SchmidtDSE/spectral_trend_database/blob/main/scripts/step-4.gap_filling_and_smoothing.py), leveraging the [spectral_trend_database.smoothing](/docs/spectral_trend_database/spectral_trend_database.smoothing.html) module.


| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | agricultural year (from Sept 1. of the prior year, through December 1 of the listed year) |
| `date` | list of dates for each value |
| `<index>` | one of: ndvi, ndbr, ndmi, ndwi, msi, rdi, srmir, slavi, wdrvi, bwdrvi, savi, gsavi, mnli, tdvi, evi, evi2, evi22, atsavi, afri1600, cm, cig, gndvi, msavi, gvi, wet, tvi, osavi, rdvi, rvi, grvi, si, si1, gari, gli, msr, nli (see [config/spectral_indices/v1](https://github.com/SchmidtDSE/spectral_trend_database/blob/main/config/spectral_indices/v1.yaml) for details) |


---

<a name='macd'></a>
### `MACD_INDICES_V1`

Exponential Moving Averages (ema), and Moving Averge Convergence Divergence (macd/macd-div) values computed from smoothed values of NDVI, EVI and EVI2. These have been [shown](https://doi.org/10.1016/j.rse.2020.111752) to be useful metrics in determining green-up dates.

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `date` | list of dates for each value |
| `year` | agricultural year (from Sept 1. of the prior year, through December 1 of the listed year) |
| `<metric>_<index>` | where `<metric>` is one of ema_(a|b|c), macd, macd-div (see the referenced [paper](https://doi.org/10.1016/j.rse.2020.111752) for more details) and `<index>` is one of the above listed spectral indices |


<a name='indices_stats'></a>
### `INDICES_STATS_V1 (_GROWING/OFF_SEASON)`

> CHANGE NOTICE: `INDICES_STATS_V1` currently uses all availble data within row. This will be limited to December 1st of the prior year to December 1st of the listed year in future versions.

Annual and sub-annual aggregation statistics (min, max, mean, median, skew, kurtosis) determined for each index in `SMOOTHED_INDICES_V1`.  `INDICES_STATS_V1` uses all available data. `INDICES_STATS_V1_OFF_SEASON` uses data from December 1st of the prior-year to March 15th of the listed year. `INDICES_STATS_V1_GROWING_SEASON` uses data from April 15th to November 1st of the listed year.

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `date` | list of dates for each value |
| `year` | agricultural year (from Sept 1. of the prior year, through December 1 of the listed year) || `<stat>` | spectral indices |
| `<index>_<stat_metric>` | where `<index>` is one of the above listed spectral indices and `<stat_metric>` is one of min, max, mean, median, skew, kurtosis |

