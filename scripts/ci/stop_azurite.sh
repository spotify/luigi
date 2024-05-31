#!/usr/bin/env bash
docker stop "$(docker ps -q --filter ancestor=mcr.microsoft.com/azure-storage/azurite)"