For maintainers of Luigi, who have push access to pypi. Here's how you upload
Luigi to pypi.

#. Make sure [twine](https://pypi.org/project/twine/) is installed ``pip install twine``.
#. Update version number in `luigi/__meta__.py`.
#. Commit, perhaps simply with a commit message like ``Version x.y.z``.
#. Push to GitHub at [spotify/luigi](https://github.com/spotify/luigi).
#. Clean up previous distributions by executing ``rm -rf dist``
#. Build a source distribution by executing ``python setup.py sdist``
#. Upload to pypi by executing ``twine upload dist/*``
#. Add a tag on github (https://github.com/spotify/luigi/releases),
   including a handwritten changelog, possibly inspired from previous notes.

Currently, Luigi is not released on any particular schedule and it is not
strictly abiding semantic versioning.
