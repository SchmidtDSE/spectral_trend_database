(scripts)=
# Scripts

---

_note: this section describes various scripts used to generate data and will likely not be part of any public facing document_

---

**[0] EXPORT LANDSAT DATA**

1. Load Sample Yield data
2. Add geohashes
3. For each year:
    - Add Harmonized Landsat Pixel Values
    - Save results, local and GCS, as line-deliminated JSON files

**[1] PREPROCESS RAW DATA**

1. Load and Concatenate CSVs from GCP
2. Remove missing band values (tested using green only)
3. Requrie unique lon-lat per geohash-7
4. remove nan/none values from coord-arrays
5. require `c.MIN_REQUIRED_YEARS` per geohash
6. Add County/State Data
7. Save results, local and GCS, as line-deliminated JSON


**[2] BIGQUERY TABLES**

1. break data into (unique/sorted) subsets and save to GCS
2. create tables based on subsets saved on GCS


**[3] RAW SPECTRAL INDICES**

1. create table for raw spectral indices
2. for each year (2000-2022):
    - compute all indices
    - save to gcs
    - save as bigquery table


**[4] GAP FILLING AND SMOOTHING**

1. ...


**[5] COVER CROP FEATURES**

1. for smoothed daily indices compute:
    a. macd(div) features


**[6] INDICIES STATS**

1. ...