# harvesting-cleaning-service

Microservice that cleans up previous successful jobs (keeps only the most recent successful one );
It also periodically remove old failed jobs.

## Installation

To add the service to your stack, add the following snippet to docker-compose.yml:

```
services:
  harvesting-cleaning:
    image: lblod/harvesting-cleaning-service:x.x.x
    volumes:
      - ./data/files:/share
```

## Configuration

### Delta

```
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status'
      },
      object: {
        type: 'uri',
        value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'
      }
    },
    callback: {
      method: 'POST',
      url: 'http://harvesting-cleaning/delta'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  },
```

This service will filter out <http://redpencil.data.gift/vocabularies/tasks/Task> with operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/cleaning>.

### Environment variables

- HIGH_LOAD_DATABASE_ENDPOINT: (default: `http://virtuoso:8890/sparql`) endpoint to use for most file related queries (avoids delta overhead)
- MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS: (default: 30) number of days to keep successful jobs

## REST API

### POST /delta

Starts the import of the given harvesting-tasks into the db

- Returns `204 NO-CONTENT` if no harvesting-tasks could be extracted.

- Returns `200 SUCCESS` if the harvesting-tasks where successfully processes.

- Returns `500 INTERNAL SERVER ERROR` if something unexpected went wrong while processing the harvesting-tasks.

## Model

See [lblod/job-controller-service](https://github.com/lblod/job-controller-service)
