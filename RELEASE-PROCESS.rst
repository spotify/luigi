For maintainers of Luigi, who have push access to pypi. Here's how you upload
Luigi to pypi.

#. Make sure [uv](https://github.com/astral-sh/uv) is installed ``curl -LsSf https://astral.sh/uv/install.sh | sh``.
#. Update version number in `luigi/__version__.py`.
#. Commit, perhaps simply with a commit message like ``Version x.y.z``.
#. Push to GitHub at [spotify/luigi](https://github.com/spotify/luigi).
#. Clean up previous distributions by executing ``rm -rf dist``.
#. Build a source distribution by executing ``uv build``.
#. Set pypi token on environment variable ``export UV_PUBLISH_TOKEN="LUIGI_PYPI_TOKEN_HERE"``.
#. Upload to pypi by executing ``uv publish``.
#. Add a tag on github (https://github.com/spotify/luigi/releases),
   including a handwritten changelog, possibly inspired from previous notes.

Currently, Luigi is not released on any particular schedule and it is not
strictly abiding semantic versioning. Whenever possible, bump major version when you make incompatible API changes, minor version when you add functionality in a backwards compatible manner, and patch version when you make backwards compatible bug fixes.
