#!/usr/bin/env bash

set -ex

ENDENV=$(echo $TOXENV | tail -c 7)
if [[ $ENDENV == gcloud ]]
then
  [[ $DIDNT_CREATE_GCP_CREDS = 1 ]] || tox
else
  tox --hashseed 1
fi
