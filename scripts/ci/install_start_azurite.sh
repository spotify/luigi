#!/usr/bin/env bash

echo "$DOCKERHUB_TOKEN" | docker login -u spotifyci --password-stdin

docker pull mcr.microsoft.com/azure-storage/azurite
mkdir -p blob_emulator
$1/stop_azurite.sh
docker run -p 10000:10000 -v blob_emulator:/data -e AZURITE_ACCOUNTS=devstoreaccount1:YXp1cml0ZQ== -d mcr.microsoft.com/azure-storage/azurite azurite-blob -l /data --blobHost 0.0.0.0 --blobPort 10000
