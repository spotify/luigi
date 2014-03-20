Writing Documentation
=====================

Luigi uses [Sphinx](http://sphinx-doc.org/) to generated documentation. Sphinx
can be installed via `pip install sphinx` after which `html` docs can be
generated with `make html`.

Sphinx uses ReStructuredText (RST) markup. There's a good
[quickstart](http://docutils.sourceforge.net/docs/user/rst/quickstart.html)
describing the syntax. We also use the sphinx [autodoc](http://sphinx-
doc.org/ext/autodoc.html) functionality to parse docstrings. For examples of
cross-referencing modules/libraries/classes and for documentatingfunction/method
arguments, see docs on [the python domain](http://sphinx-doc.org/domains.html
#the-python-domain).
