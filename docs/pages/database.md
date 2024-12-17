# Database

<!-- start_db_overview -->

> CHANGE NOTICE: We are still working on operationalization. The database currently only goes through 2001

The _Spectral Trend Database_ tracks over 14 thousand points in the mid-western United States from 2000 to present. Below we have a detailed description of each of the tables. The database lives on [Google Big Query](https://cloud.google.com/bigquery/docs) and can be accessed directly using big query.  However, we've built a number of python tools to make accessing the data eaiser ([docs](XXX), [example](XXX)).

---

<a name='overview'></a>
| Table | Keys | Dates | Daily | Description |
| ---: | :----: | :----: | :----: | :---- |
|  [SAMPLE_POINTS](/pages/database.html/#sample_points) | sample_id | False | False | location information such as lat, lon and geohashes |
|  [ADMINISTRATIVE_BOUNDARIES](/pages/database.html/#admin_boundaries) | sample_id | False | False | administrative information such as state and county |
|  [QDANN_YIELD](/pages/database.html/#qdann_yield) | sample_id, year | True | False | yield estimations for year |
|  [LANDSAT_RAW_MASKED](/pages/database.html/#masked_landsat) | sample_id, year | True | False | masked landsat band values for year |
|  [RAW_INDICES_V1](/pages/database.html/#raw_indices) | sample_id, year | True | False | spectral indices built from `LANDSAT_RAW_MASKED`|
|  [SMOOTHED_INDICES_V1](/pages/database.html/#indices) | sample_id, year | True | True | interpolated and smoothed daily values for indices contained in `RAW_INDICES_V1` |
|  [MACD_INDICES_V1](/pages/database.html/#macd) | sample_id, year | True | True |  additional indices dervived from `SMOOTHED_INDICES_V1` whose values are useful for detecting cover-croping and green-up dates |
|  [INDICES_STATS_V1](/pages/database.html/#indices_stats) | sample_id, year | True | False | statistical (min, max, mean, median, skew, kurtosis) aggregation of `SMOOTHED_INDICES_V1` |
|  [INDICES_STATS_V1_GROWING_SEASON](/pages/database.html/#indices_stats) | sample_id, year | True | False | same as `INDICES_STATS_V1` but restricted to the "growing season" |
|  [INDICES_STATS_V1_OFF_SEASON](/pages/database.html/#indices_stats) | sample_id, year | True | False | same as `INDICES_STATS_V1` but restricted to the "off season"|

<!-- end_db_overview -->

---

<a name='sample_points'></a>
## `SAMPLE_POINTS`


> CHANGE NOTICE: sample_id, and geohash references with be replaced with [h3](https://h3geo.org/) in future versions

Location information such as lat, lon and geohashes. Note the columns are redundant, as `sample_id` is just an 11-character [geohash](https://en.wikipedia.org/wiki/Geohash), and determines the lat, lon with sufficient accuracy.


| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
|  `geohash_<n>` (`n` in [5, 7, 9]) | additional geohashes redudant with `sample_id`. originally included for initial parameter searches and will be removed in future versions |
|  lon | longitude |
|  lat | latitude |


<a name='admin_boundaries'></a>
## `ADMINISTRATIVE_BOUNDARIES`

Administrative information, such as state and county, taken from [XXX](XXX).


| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `AWATER` | Total Water Area for XXX |
| `ALAND` | Total Land Area for XXX  |
| `LSAD` | XXX |
| `STATE_NAME` | State Name |
| `STUSPS` | XXX |
| `NAME` | XXX |
| `GEOID` | XXX |
| `GEOIDFQ` | XXX |
| `COUNTYFP` | XXX |
| `NAMELSAD` | XXX |
| `COUNTYNS` | XXX |
| `STATEFP` | XXX |

<a name='qdann_yield'></a>
## `QDANN_YIELD`

Modeled yield data from 200X - Present using [QDANN](XXX).


| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to submeter precision |
| `year` | year of yield |
| `` |  |
| `` |  |
| `` |  |
| `` |  |
| `` |  |

<a name='masked_landsat'></a>
## `LANDSAT_RAW_MASKED`

> CHANGE NOTICE: In upcoming versions of STDB we will likely replace list-valued date/band row for each year, with independent rows for each day

Masked Landsat (optical) band values. The columns `B<n>` contain lists for each date where data is available.  There is a corresponding `date` column containing a list of dates for each value.

See [XXX](XXX) for tools and examples of how to convert each row to an `xr.Dataset`

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | year of yield |
| `date` |  |
| `B<n>`  (n in [1-12])|  |


<a name='raw_indices'></a>
## `RAW_INDICES_V1`

Spectral indices derived using the band-values in `LANDSAT_RAW_MASKED`. See [XXX](XXX) for a list of indices (`index`) included.

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | year of yield |
| `<index>` | spectral indices |



<a name='indices'></a>
## `SMOOTHED_INDICES_V1`

XXX

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | year of yield |
| `<index>` | spectral indices |



<a name='macd'></a>
## `MACD_INDICES_V1`

XXX

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | year of yield |
| `<index>` | spectral indices |



<a name='indices'></a>
## `MACD_INDICES_V1`

XXX

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | year of yield |
| `<index>` | spectral indices |

<a name='indices_stats'></a>
## `SMOOTHED_INDICES_V1 (GROWING/OFF_SEASON)`

XXX

| Column(s) | Description |
| ---: | :---- |
|  `sample_id` | (key-column) 11-character geohash. specifies location down to about 15 cm. |
| `year` | year of yield |
| `<index>` | spectral indices |


