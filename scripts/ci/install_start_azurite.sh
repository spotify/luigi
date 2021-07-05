#!/usr/bin/env bash

echo "$DOCKERHUB_TOKEN" | docker login -u spotifyci --password-stdin

docker pull arafato/azurite
mkdir -p blob_emulator
$1/stop_azurite.sh
docker run -e executable=blob -d -t -p 10000:10000 -v blob_emulator:/opt/azurite/folder arafato/azurite
