# Docker
This is the entry-point for getting the luigi system running with Docker.

## Build Staging Scheduler Locally

### Setup
You will FIRST need to make a file named `client.cfg` and put it inside `/luigi/docker/staging-scheduler/etc/luigi/client.cfg`.

```
[scheduler]
record_task_history: True
state-path: /luigi/state/luigi-state.pickle

[task_history]
db_connection: sqlite:////luigi/state/luigi-task-history.db
```

### Run
To run the staging scheduler locally, we must build it and then run it WHILE forwarding a PORT to open it on locally.

```bash
cd /luigi/docker/staging-scheduler
docker build -t my-tag .
bocker run -p 8082:8082 my-tag
```
