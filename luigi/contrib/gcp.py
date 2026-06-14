"""
Common code for GCP (google cloud services) integration
"""

import logging

logger = logging.getLogger("luigi-interface")

try:
    import google.auth
    import httplib2

    _gcp_enabled = True
except ImportError:
    _gcp_enabled = False


def get_authenticate_kwargs(oauth_credentials=None, http_=None):
    """Returns a dictionary with keyword arguments for use with discovery

    Prioritizes oauth_credentials or a http client provided by the user
    If none provided, falls back to default credentials provided by google's command line
    utilities. If that also fails, tries using httplib2.Http()

    Used by `gcs.GCSClient` and `bigquery.BigQueryClient` to initiate the API Client
    """
    if not _gcp_enabled:
        raise ImportError(
            "google-auth and google-auth-httplib2 are required for GCP functionality. Install them with: pip install google-auth google-auth-httplib2"
        )
    if oauth_credentials:
        authenticate_kwargs = {"credentials": oauth_credentials}
    elif http_:
        authenticate_kwargs = {"http": http_}
    else:
        # neither http_ or credentials provided
        try:
            # try default credentials
            credentials, _ = google.auth.default()
            authenticate_kwargs = {"credentials": credentials}
        except google.auth.exceptions.DefaultCredentialsError:
            # try http using httplib2
            authenticate_kwargs = {"http": httplib2.Http()}

    return authenticate_kwargs
