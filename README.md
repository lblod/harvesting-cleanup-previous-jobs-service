# harvesting-cleaning-service

Microservice that cleans up old jobs based on configurable retention periods.
It removes successful jobs older than the configured threshold and failed jobs older than their configured threshold.
When cleaning a job, it also deletes all associated files (both physical files on disk and metadata in the database).

## Installation

To add the service to your stack, add the following snippet to docker-compose.yml:

```
services:
  harvesting-cleaning:
    image: lblod/harvesting-cleanup-previous-jobs-service
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

- `HIGH_LOAD_DATABASE_ENDPOINT`: (default: `http://virtuoso:8890/sparql`) endpoint to use for most file related queries (avoids delta overhead)
- `MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS`: (default: 30) number of days to keep successful jobs before they are eligible for deletion
- `MAX_DAYS_TO_KEEP_BUSY_JOBS`: (default: 7) number of days to keep busy jobs before they are eligible for deletion
- `MAX_DAYS_TO_KEEP_FAILED_JOBS`: (default: 7) number of days to keep failed jobs before they are eligible for deletion
- `DEFAULT_GRAPH`: (default: "http://mu.semte.ch/graphs/harvesting") the default graph where job triples are stored

### Scope the cleanup to specific job operations

By default, all jobs are cleaned up regardless of their operation. To filter cleanup to specific job operations only, add them to your project `config/cleaning/config.json` e.g.:

```json
{
  "jobOperations": [
    "http://lblod.data.gift/id/jobs/concept/JobOperation/harvesting",
    "http://lblod.data.gift/id/jobs/concept/JobOperation/importing"
  ]
}
```

And mount the config directory in the docker-compose.yml:

```
services:
  harvesting-cleaning:
    image: lblod/harvesting-cleanup-previous-jobs-service
    volumes:
      - ./data/files:/share
      - ./config/cleaning:/config
```

Job operations are defined as:

```sparql
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
?job
  a cogs:Job ;
  task:operation ?jobOperation .
```

## REST API

### POST /delta

Starts the import of the given harvesting-tasks into the db

- Returns `204 NO-CONTENT` if no harvesting-tasks could be extracted.

- Returns `200 SUCCESS` if the harvesting-tasks where successfully processes.

- Returns `500 INTERNAL SERVER ERROR` if something unexpected went wrong while processing the harvesting-tasks.

## Model

See [lblod/job-controller-service](https://github.com/lblod/job-controller-service)
