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

XXX IMAGE HERE OF BEFORE AND AFTER

The gap filling and smoothing is managed our [savitzky_golay_processor](/docs/spectral_trend_database/spectral_trend_database.smoothing.html#spectral_trend_database.smoothing.savitzky_golay_processor). The following steps are taken:

1. Expand time series to a daily-dataset inserting `nan` for missing values.
2. Perform Linear interpolation to replace `nan` values
3. Remove points where the time-series has a large drop. Specifically, we create a smoothed curve by performing symmetrical mean smoothing over a 32 day window. We then remove points where the time-series data divide by the smoothed data is less than 0.5.
4. Replace the removed points using linear interpolation
5. Apply a Savitzky Golay filter. Namely the scipy.savgol_filter with window length 60, and poly-order 3.


(macd)=
### Moving Average Convergence Divergence (Divergence)

Exponential Moving Averages (EMA), and Moving Average Convergence Divergence curves have been [shown](https://doi.org/10.1016/j.rse.2020.111752) to be useful metrics in determining green-up dates. We've included them in our database for this specific purpose, however we also expect that the derived features may well be useful for other applications in studying agricultural trends.

$$
ewm_a - ewm_b
macd_values - ewm_c
$$

EMA is most often written in the recursive form, $s_{t} =\alpha x_{t}+(1-\alpha )s_{t-1}$, which is wonderful and quick when updating a series.
However we are interested in examining existing series and would like to vectorize.  We'll start by expanding out the "$t$-th" term (using wikipedia as a reference), and then recollect our terms in a more useful form for computation:
$$
{\displaystyle {
    \begin{aligned}
        s_{t}&=\alpha x_{t}+(1-\alpha )s_{t-1} \\[3pt]
             &=\alpha x_{t}+\alpha (1-\alpha )x_{t-1}+(1-\alpha )^{2}s_{t-2} \\[3pt]
             &=\alpha \left[x_{t}+(1-\alpha )x_{t-1}+(1-\alpha )^{2}x_{t-2}+(1-\alpha )^{3}x_{t-3}+\cdots +(1-\alpha )^{t-1}x_{1}\right]+(1-\alpha )^{t}x_{0} \ \ \ \ \text{(en.wikipedia.org/wiki/Exponential\_smoothing)}\\[3pt]
             &=\alpha \left[\sum_{p=0}^{t-1} x_{t-p} (1-\alpha )^{p}\right]  + (1-\alpha )^{t}x_{0} \\[3pt]
             &=\alpha (1-\alpha )^{t} \left[\sum_{p=0}^{t-1} x_{t-p} (1-\alpha )^{p-t}\right]  + (1-\alpha )^{t}x_{0} \\[3pt]
             &=\alpha (1-\alpha )^{t} \left[\sum_{q=1}^{t} x_{q} (1-\alpha )^{-q}\right]  + (1-\alpha )^{t}x_{0}\ \ ; \ \ \ \ q\equiv(t-p) \\[3pt]
             &=\alpha (1-\alpha )^{t} \left[\sum_{q=1}^{t} x_{q} (1-\alpha )^{-q}  + \frac{x_{0}}{\alpha}\right] \\[3pt]
             &=\alpha (1-\alpha )^{t} \left[\sum_{q=1}^{t} x_{q} (1-\alpha )^{-q}  + \tilde{x}_{0}\right]\ \ ; \ \ \ \ \tilde{x}_{0}\equiv\frac{x_{0}}{\alpha}\\[3pt]
             &=\alpha (1-\alpha )^{t} \left[\sum_{q=0}^{t} \tilde{x}_{q} (1-\alpha )^{-q}\right]\ \ ; \ \ \ \ \tilde{x} \equiv [\tilde{x}_{0}, x_{1}, x_{2}, \cdots ]\\[3pt]
    \end{aligned}
}}
$$
We can now write this in vector form where $s = \left[ s_0, s_1, \cdots s_{N} \right]$:
$$
{\displaystyle {
    \begin{aligned}
            s&=\alpha\ (1-\alpha)^{\overline{N}} \text{CUMSUM}[\tilde{x} \times (1-\alpha)^{-\overline{N}} ] \\[3pt]
            \overline{N}&\equiv[0,1,2,\cdots N]\ \ ; \ \ \ \ N+1 = \text{size of }\tilde{x}
    \end{aligned}
}}
$$

This final form can be easily implemneted in numpy.

It's worth noting however that $(1-\alpha)^{\pm t}$ terms quickly become infintesimal or expload leading to problematic overflow errors.  Depending on your choice of $\alpha$ they appear around $t \sim 2000$.  Our series are much smaller than this and its therefore not an issue. Nonetheless, it's  worth noting how we might fix the issue. Naievly we could just replace these terms with $0$ at suffictiently large $t$. If we want to only keep terms of order $t_{max}$ we need to make sure that we only keep the terms in the summation who's total power is less that $t_{max}$.  The most straight forward way to to this is to return to the "geometric" form we've copied from wikipedia. Then for a given $\alpha$ and max-precision compute $t_{max}$ and force all terms with $t>t_{max}$ to zero. A better approach might be to consider the "effective window size" (`span` in the below code).  The idea is that the vast majority of the contributions come from this window.  If we took some batch size of `M * span` for sufficiently large `M` (lets say 50) we could calculate ewma in sequential batches, avoid overflow errors with negligible contributions. The (relative) scale of these approximation would be $(1-\alpha)^{M*span}$


