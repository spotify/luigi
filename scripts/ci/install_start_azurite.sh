#!/usr/bin/env bash
AZURITE_IMAGE=mcr.microsoft.com/azure-storage/azurite
docker pull $AZURITE_IMAGE
mkdir -p blob_emulator
$1/stop_azurite.sh
docker run -e executable=blob -d -t -p 10000:10000 -v blob_emulator:/opt/azurite/folder $AZURITE_IMAGE
