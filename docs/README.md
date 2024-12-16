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
3. use autodoc
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
	'sphinx.ext.autodoc']


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

Build API DOCS

``` bash
# _docs is output directory. ignore in gitignore
sphinx-apidoc -o _docs ../spectral_trend_database/
```

Make HTML (note this should be re-run each time you do a new build)

``` bash
make html
```

Local Server

``` bash
python -m http.server -d _build/html 8000
```

