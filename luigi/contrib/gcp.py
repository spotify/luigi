"""
Common code for GCP (google cloud services) integration
"""
import logging
logger = logging.getLogger('luigi-interface')

try:
    import httplib2
    import google.auth
except ImportError:
    logger.warning("Loading GCP module without the python packages httplib2, google-auth. \
        This *could* crash at runtime if no other credentials are provided.")


def get_authenticate_kwargs(oauth_credentials=None, http_=None):
    """Returns a dictionary with keyword arguments for use with discovery

    Prioritizes oauth_credentials or a http client provided by the user
    If none provided, falls back to default credentials provided by google's command line
    utilities. If that also fails, tries using httplib2.Http()

    Used by `gcs.GCSClient` and `bigquery.BigQueryClient` to initiate the API Client
    """
    if oauth_credentials:
        authenticate_kwargs = {
            "credentials": oauth_credentials
        }
    elif http_:
        authenticate_kwargs = {
            "http": http_
        }
    else:
        # neither http_ or credentials provided
        try:
            # try default credentials
            credentials, _ = google.auth.default()
            authenticate_kwargs = {
                "credentials": credentials
            }
        except google.auth.exceptions.DefaultCredentialsError:
            # try http using httplib2
            authenticate_kwargs = {
                "http": httplib2.Http()
            }

    return authenticate_kwargs
