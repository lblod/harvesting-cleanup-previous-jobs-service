import { sparqlEscapeUri, sparqlEscapeDateTime } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import {
  DEFAULT_GRAPH,
  MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS,
  MAX_DAYS_TO_KEEP_BUSY_JOBS,
  MAX_DAYS_TO_KEEP_FAILED_JOBS,
  HIGH_LOAD_DATABASE_ENDPOINT,
  JOB_OPERATIONS,
  STATUS_SUCCESS,
  STATUS_BUSY,
  STATUS_FAILED,
} from '../constants';

const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};

// This function is no longer needed as we're not using dump file logic
// Keeping it for backward compatibility but it returns a far past date
export async function getLastDumpFileJobDate() {
  console.log(
    'Dump file logic is deprecated - using date-based retention only',
  );
  return new Date(0); // we keep everything based on date only (Since 1970 ;-P)
}

function jobOperationFilter(operations) {
  if (operations.length === 0) {
    return '';
  } else {
    return `VALUES ?jobOperation {${operations
      .map((o) => sparqlEscapeUri(o))
      .join(' ')}}`;
  }
}

export async function getJobWithStatusAndBeforeDate(status, date) {
  const q = `
    ${PREFIXES}
    SELECT DISTINCT ?job WHERE {
      ${jobOperationFilter(JOB_OPERATIONS)}
      GRAPH <${DEFAULT_GRAPH}> {
        ?job a ?type;
             adms:status ${sparqlEscapeUri(status)};
             dct:modified ?modified.
        FILTER (?modified < ${sparqlEscapeDateTime(date)} && ?type IN (cogs:Job, cogs:ScheduledJob))

        ${JOB_OPERATIONS.length > 0 ? '?job task:operation ?jobOperation.' : ''}
      }
    }`;
  let res = await query(q, {}, connectionOptions);
  return res.results.bindings.map((r) => r.job.value);
}

export async function getSuccessfulJobsBeforeDate() {
  const maxDaysToKeepSuccessfulJobs = new Date();
  maxDaysToKeepSuccessfulJobs.setDate(
    maxDaysToKeepSuccessfulJobs.getDate() - MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS,
  );

  const q = `
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX cogs: <http://vocab.deri.ie/cogs#>

    SELECT DISTINCT ?job WHERE {
      ${jobOperationFilter(JOB_OPERATIONS)}
      GRAPH <${DEFAULT_GRAPH}> {
        ?job a ?type;
             adms:status ${sparqlEscapeUri(STATUS_SUCCESS)};
             dct:modified ?modified.
        FILTER (?modified < ${sparqlEscapeDateTime(maxDaysToKeepSuccessfulJobs)} && ?type IN (cogs:Job, cogs:ScheduledJob))

        ${JOB_OPERATIONS.length > 0 ? '?job task:operation ?jobOperation.' : ''}
      }
    }`;

  const res = await query(q, {}, connectionOptions);
  const jobsToClean = res.results.bindings.map((r) => r.job.value);

  console.log(
    `Found ${jobsToClean.length} successful jobs older than ${maxDaysToKeepSuccessfulJobs.toISOString()}`,
  );

  return jobsToClean;
}

export async function countFileForJob(jobUri) {
  const q = `
    ${PREFIXES}
    SELECT (COUNT(DISTINCT ?file) AS ?files)
    WHERE {
      GRAPH <${DEFAULT_GRAPH}> {
        ?task dct:isPartOf ${sparqlEscapeUri(jobUri)}.
        ?container a task:DataContainer.
        {
          ?task task:resultsContainer ?container.
        } UNION {
          ?task task:inputContainer ?container.
        }
        ?container task:hasFile ?file.
      }
    }
`;
  const res = await query(q, {}, connectionOptions);
  return res.results.bindings[0].files.value;
}

export async function getFilesForJob(
  jobUri,
  fileHandler = async () => {
    console.log('not implemented');
  },
) {
  const limit = 5000;

  let nbFiles = await countFileForJob(jobUri);
  while (nbFiles > 0) {
    console.log(`cleaning job ${jobUri} with ${nbFiles} files...`);
    const q = `
      PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

      SELECT DISTINCT ?file ?fileOnDisk
      WHERE {
        {
          SELECT DISTINCT ?file ?fileOnDisk WHERE {
            GRAPH <${DEFAULT_GRAPH}> {
              ?task dct:isPartOf ${sparqlEscapeUri(jobUri)}.
              ?container a task:DataContainer.
              {
                ?task task:resultsContainer ?container.
              } UNION {
                ?task task:inputContainer ?container.
              }
              ?container task:hasFile ?file.
              ?fileOnDisk nie:dataSource ?file.
            }
          } ORDER BY ?file ?fileOnDisk
        }
      } LIMIT ${limit} OFFSET 0
    `;
    const res = await query(q, {}, connectionOptions);
    const files = res.results.bindings.map((r) => {
      return {
        file: r.file.value,
        fileOnDisk: r.fileOnDisk.value,
      };
    });
    await fileHandler(files);
    nbFiles = await countFileForJob(jobUri);
  }
}

export async function genericDelete(jobUri) {
  // First, delete all tasks and their containers related to this job
  const deleteTasksAndContainers = `
    ${PREFIXES}
    DELETE WHERE {
      GRAPH <${DEFAULT_GRAPH}> {
        ?task dct:isPartOf ${sparqlEscapeUri(jobUri)}.
        ?task ?taskP ?taskO.
        OPTIONAL {
          ?taskS ?taskSP ?task.
        }
        OPTIONAL {
          ?task task:resultsContainer ?resultsContainer.
          ?resultsContainer ?rcP ?rcO.
        }
        OPTIONAL {
          ?task task:inputContainer ?inputContainer.
          ?inputContainer ?icP ?icO.
        }
      }
    }`;

  await update(deleteTasksAndContainers, {}, connectionOptions);

  // Then, delete the job itself
  const deleteJob = `
    DELETE WHERE {
      GRAPH <${DEFAULT_GRAPH}> {
        ${sparqlEscapeUri(jobUri)} ?p ?o.
        OPTIONAL {
          ?s ?sp ${sparqlEscapeUri(jobUri)}.
        }
      }
    }`;

  await update(deleteJob, {}, connectionOptions);
}

export async function deleteFileInDb(f) {
  const askByPredicate = async (pred) => {
    const res = await query(
      `
      ASK WHERE {
        GRAPH <${DEFAULT_GRAPH}> {
          ?s <${pred}> ${sparqlEscapeUri(f)}; ?p ?o .
        }
      }`,
      {},
      connectionOptions,
    );
    return res.boolean;
  };
  const deleteByPredicate = async (pred) => {
    while (await askByPredicate(pred)) {
      await update(
        `
        DELETE WHERE {
          GRAPH <${DEFAULT_GRAPH}> {
            ?s <${pred}> ${sparqlEscapeUri(f)}; ?p ?o .
          }
        } LIMIT 1000`,
        {},
        connectionOptions,
      );
    }
  };
  await deleteByPredicate(
    'http://redpencil.data.gift/vocabularies/tasks/hasFile',
  );
  await deleteByPredicate(
    'http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource',
  );
  await deleteByPredicate('http://oscaf.sourceforge.net/ndo.html#copiedFrom');
  await deleteByPredicate('http://purl.org/dc/terms/hasPart');
  await update(
    `
    DELETE WHERE {
      GRAPH <${DEFAULT_GRAPH}> {
        ${sparqlEscapeUri(f)} ?p ?o .
      }
    }`,
    {},
    connectionOptions,
  );
}
