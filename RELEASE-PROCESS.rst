For maintainers of luigi, who have push access to pypi. Here's how you upload
luigi to pypi.

     1. Update version number in setup.py, if needed.
     2. pypi (Executing `python setup.py sdist upload`)
     3. Add tag on github (https://github.com/spotify/luigi/releases)
     4. Then bump verson number in setup.py, "preparing for next release".

If you know a better way, please say so! I'm (arash) not used to releasing code
to pypi!

Currently, luigi is not released on any particular schedule and it is not
strictly abiding semantic versioning.
