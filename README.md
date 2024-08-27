####  SPECTRAL TREND DATABASE

MODULES AND SCRIPTS TO:

1. export landsat/s2/yield sample data from GEE [will return to this]
2. preprocess and put into BQ [will return to this]

3. add raw spectral index data
	- configuration set up
	- nice cli
4. smooth indices and bands [TUESDAY]

5. compute features: [THURSDAY]
	- feats in early detection for cover crops
	- green (or wet or soil ... days)
	- other

---

#### INSTALL

```python
...
```

--- 

#### REQUIREMENTS

Requirements are managed through a conda yaml [file](./conda-env.yaml). To create/update the `ENV_NAME` environment:

```bash
# create
mamba env create -f conda-env.yaml

# update
mamba env update -f conda-env.yaml --prune
```

--- 

#### QUICK START

Usage example

---

#### DOCUMENTATION

API Docss

--- 

#### STYLE-GUIDE

Following PEP8. See [setup.cfg](./setup.cfg) for exceptions. Keeping honest with `pycodestyle .`


