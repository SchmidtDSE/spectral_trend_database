(examples)=
# Examples

Before diving into how the data was produced and processed, let's take a look at a few examples of how to use the [`spectral_trend_database`](/docs/index.html) module.

---

## Querying the Database

The data is in a big-query database, and so can be accessed using the [big-query api](https://cloud.google.com/bigquery/docs).


:::{embed} #nb.example_query_bq
:::

Here is the same query using [query.QueryConstructor](/docs/spectral_trend_database/spectral_trend_database.query.html#spectral_trend_database.query.QueryConstructor)


:::{embed} #nb.example_query_stdb_basic
:::

The real benefit, however, is in constructing SQL queries with multiple `JOIN` and `WHERE` statements. Here is a more complicated request collecting spectral index data from 2012-2015 for a subset of sample_ids:

:::{embed} #nb.example_query_stdb_advanced
:::

---


## Parsing the data

Using the [utils](/docs/spectral_trend_database/spectral_trend_database.utils.html) module we can easily turn these rows into xarray-datasets to parse and interact with the data.

:::{embed} #nb.parsing_single_row
:::

Similarly we can turn multiple rows into a single dataset. Because rows contain overlapping dates we'll also need to filter the dates using the `filter_dates` method below:

:::{embed} #nb.parsing_multiple_rows
:::

---

## Computations and Visualizations

This workflow allows us to efficiently perform computations and visualize the data. Here's a quick visualization of our data of NDVI and the NDVI exponentially weighted moving average (with a 10 day window). We can then additionally add to the visualization a computed trend, namely the difference between the normalized difference vegetation index and the normalized difference water index.

:::{embed} #nb.ndvi_vs_ema
:::

