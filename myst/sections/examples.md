(examples)=
# Examples

Before diving into how the data was produced and processed, let's take a look at a few examples of how to use the [`spectral_trend_database`](/spectral_trend_database/docs) module.

---

## Querying the Database

The data is in a big-query database, and so can be accessed using the [big-query api](https://cloud.google.com/bigquery/docs).


:::{embed} #nb.example_query_bq
:::

Here is the same query using [query.QueryConstructor](/spectral_trend_database/docs/docs/spectral_trend_database/spectral_trend_database.query.html#spectral_trend_database.query.QueryConstructor)


:::{embed} #nb.example_query_stdb_basic
:::

The real benefit, however, is in constructing SQL queries with multiple `JOIN` and `WHERE` statements. Here is a more complicated request collecting spectral index data from 2010-2013 for a subset of sample_ids:

:::{embed} #nb.example_query_stdb_advanced
:::

---


## Parsing the data

Using the [utils](/spectral_trend_database/docs/docs/spectral_trend_database/spectral_trend_database.utils.html) module we can easily turn these rows into xarray-datasets to parse and interact with the data.

:::{embed} #nb.rows_to_xr
:::

And then filter by dates

:::{embed} #nb.filter_by_dates
:::

---

## Computations and Visualizations

This workflow allows us to efficiently perform computations and visualize the data. Here's a quick visualization of NDVI and it's exponentially weighted moving average (EWMA). Additionally we have added to the visualization a computed trend, namely the difference between the NDVI and it's EWMA, in order to elucidate the magnitude of the shift.

:::{embed} #nb.ndvi_vs_ema
:::

