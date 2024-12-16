# SPHINX SETUP & DEV NOTES

Links:

* [sphinx](https://www.sphinx-doc.org/en/master/usage/quickstart.html)
* [auto-doc](https://www.sphinx-doc.org/en/master/tutorial/automatic-doc-generation.html)

---

Getting Started

```bash
# from repo root
mkdir -p docs/pages
cd docs
sphinx-quickstart
```

Replace `index.rst` with `index.md`. Here's an [example](https://raw.githubusercontent.com/pradyunsg/furo/refs/heads/main/docs/index.md) from the Furo theme.


---

Edit `conf.py` to:

1. add repo root directory
2. add extensions:
	a. myst_parser: accept markdown
    b. [sphinx.ext.viewcode](https://www.sphinx-doc.org/en/master/usage/extensions/viewcode.html): links to hightlighted src code
    c. [sphinx.ext.napoleon](https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html): numpy/google doc strings
    d. [sphinx.ext.autosummary](https://www.sphinx-doc.org/en/master/usage/extensions/autosummary.html): note sure what this is doing
	e. [sphinx.ext.autodoc](https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html): add auto-doc
3. use output_dir `docs/` note this should be added to `.gitignore` so it is created on gh-action
4. add the pacage for autodoc2
3. use markdown for `.md/.text` files
4. use [furo-theme](https://sphinx-themes.org/sample-sites/furo/)

```python
import os
import sys
sys.path.insert(0, os.path.abspath('..'))

...

extensions = [
	'myst_parser',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.autosummary',
	'autodoc2']


autodoc2_output_dir = 'docs'

autodoc2_packages = [
    {
        "path": "../spectral_trend_database",
        "auto_mode": True,
    }
]

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}

...

html_theme = 'furo'
```

---

## DEVELOPMENT

Build API DOCS and make HTML (note autodoc2 automatically runs on `make`)

``` bash
make html
```

Local Server

``` bash
python -m http.server -d _build/html 8000
```

