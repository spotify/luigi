For maintainers of Luigi, who have push access to pypi. Here's how you upload
Luigi to pypi.

     1. Update version number in setup.py, if needed.
     2. Update version number in debian/changelog
       * Use `date -R` to retrieve date
       * Optionally verify with `dpkg-parsechangelog`
     3. Commit and push.
     4. pypi (Executing ``python setup.py sdist upload``)
     5. Add tag on github (https://github.com/spotify/luigi/releases), including changelog

If you know a better way, please say so! I'm (arash) not used to releasing code
to pypi!

Currently, Luigi is not released on any particular schedule and it is not
strictly abiding semantic versioning.
