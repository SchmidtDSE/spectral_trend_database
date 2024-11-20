(intro)=
# Introduction

Spectral indices, mathematical combinations of pixel values, play an important role in remote sensing. The most well known example would be the _Normalized Difference Vegetation Index_ (NDVI):

```{math}
:label: eqn:ndvi
\text{NDVI} = \frac{\text{NIR} - \text{RED}}{\text{NIR} + \text{RED}}
```

Why is this of interest? Vegetation appears green because the vegetation reflects green light and absorbs red light. One then might assume the difference between green and red is a good measure of vegetation. An index constructed from this difference known as the _Green Normalized Difference Vegetation Index_ is useful for studying dense cannopies at later stages of developement. As described [here](https://www.soft.farm/en/blog/vegetation-indices-ndvi-evi-gndvi-cvi-true-color-140) GNDVI "is an indicator of the photosynthetic activity of the vegetation cover; it is most often used in assessing the moisture content and nitrogen concentration in plant leaves according to multispectral data which do not have an extreme red channel."

Thanks to multi-spectral satellites we are not confined to our lived experince of visible wavelengths. It turns out that the cell structures within plants also reflect Near Infrared (NIR). The difference between NIR and RED is a good indicator of the "is a measure of the amount and vigor of vegetation on the land surface" ([usda](https://ipad.fas.usda.gov/cropexplorer/Definitions/spotveg.htm)). The denominator in {eq}`eqn:ndvi` is normilization term so that $\text{NDVI} \subset [-1:1]$.

In addition to NDVI and GNDVI there are myriad other spectral indices of interest, each with its own particular use case from: measuring water content within vegetation, to detecting water bodies or human infrastrutue, and quantifying soil moisture and soil health.

The direct goal of the _Spectral Trend Database_ (STDB) is to compute and
track a large number ([36](../../config/spectral_indices/v1.yaml)) spectral-indices over corn and soy fields from 2000 to present. The database currently is based on Landsat satellites, however we are in the process of generating the same data (2018 to present) using Sentinel-2.  This data should prove useful to a large number of applications in the study of agricultural remote sensing, including the study of yeild, yeild-stablity, cover-croping and other regenerative agricultural practices and soil health.

This particular datasbase has been constructed for studying corn and soy. The general techniques, however, are applicable to a number of other scientific studies. Our open-source code base is constructed to allow the user to easily re-run these calcuations for their particular sample points, data sources, time periods, and spectral indices of interest, broadening the potential applications far beyond corn and soy, or even agricultural studies.