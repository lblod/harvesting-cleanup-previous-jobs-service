import envvar from 'env-var';
import config from '/config/config.json' with { type: 'json' };

export const TASK_HARVESTING_CLEANING =
  'http://lblod.data.gift/id/jobs/concept/TaskOperation/cleaning';

export const STATUS_BUSY =
  'http://redpencil.data.gift/id/concept/JobStatus/busy';
export const STATUS_SCHEDULED =
  'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
export const STATUS_SUCCESS =
  'http://redpencil.data.gift/id/concept/JobStatus/success';
export const STATUS_FAILED =
  'http://redpencil.data.gift/id/concept/JobStatus/failed';

export const JOB_TYPE = 'http://vocab.deri.ie/cogs#Job';
export const TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';
export const ERROR_TYPE = 'http://open-services.net/ns/core#Error';
export const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/error/';

export const PREFIXES = `
  PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
  PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
  PREFIX terms: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
`;

export const HIGH_LOAD_DATABASE_ENDPOINT = envvar
  .get('HIGH_LOAD_DATABASE_ENDPOINT')
  .default('http://virtuoso:8890/sparql')
  .asString();

export const MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS = envvar
  .get('MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS')
  .default(30)
  .asInt();

export const MAX_DAYS_TO_KEEP_BUSY_JOBS = envvar
  .get('MAX_DAYS_TO_KEEP_BUSY_JOBS')
  .default(7)
  .asInt();

export const MAX_DAYS_TO_KEEP_FAILED_JOBS = envvar
  .get('MAX_DAYS_TO_KEEP_FAILED_JOBS')
  .default(7)
  .asInt();

export const DEFAULT_GRAPH = envvar
  .get('DEFAULT_GRAPH')
  .default('http://mu.semte.ch/graphs/harvesting')
  .asString();

export const JOB_OPERATIONS = config.jobOperations;
