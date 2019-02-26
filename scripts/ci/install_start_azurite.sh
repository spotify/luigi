#!/usr/bin/env bash
docker pull arafato/azurite
mkdir -p blob_emulator
$1/stop_azurite.sh
docker run -e executable=blob -d -t -p 10000:10000 -v blob_emulator:/opt/azurite/folder arafato/azurite