For maintainers of Luigi, who have push access to pypi. Here's how you upload
Luigi to pypi.

#. Update version number in ``setup.py``.
#. Update version number in ``debian/changelog``

   - Use ``date -R`` to retrieve date
   - Optionally verify with ``dpkg-parsechangelog``
#. Commit, perhaps simply with a commit message like ``Version x.y.z``.
#. Push to GitHub at [spotify/luigi](https://github.com/spotify/luigi).
#. Upload to pypi by executing ``python setup.py sdist upload``
#. Add a tag on github (https://github.com/spotify/luigi/releases),
   including a handwritten changelog, possibly inspired from previous notes.

Currently, Luigi is not released on any particular schedule and it is not
strictly abiding semantic versioning.
