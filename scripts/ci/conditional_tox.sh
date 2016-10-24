#!/usr/bin/env bash

set -ex

ENDENV=$(echo $TOXENV | tail -c 7)
if [[ $ENDENV == gcloud ]]
then
  [[ $DIDNT_CREATE_GCP_CREDS = 1 ]] || tox -v
else
  tox --hashseed 1 -v
fi
