# Docker
This is the entry-point for getting the luigi system running with Docker.

## Build Staging Scheduler Locally
To run the staging scheduler locally, we must build it and then run it WHILE forwarding a PORT to open it on locally.

```bash
cd /luigi/docker/staging-scheduler
docker build -t my-tag .
bocker run -p 8082:8082 my-tag
```
