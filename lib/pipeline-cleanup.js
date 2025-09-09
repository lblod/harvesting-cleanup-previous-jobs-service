import { appendTaskError, loadExtractionTask, updateTaskStatus } from "./task";
import {
  MAX_DAYS_TO_KEEP_BUSY_JOBS,
  MAX_DAYS_TO_KEEP_FAILED_JOBS,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
} from "../constants";
import { rm, unlink } from "fs/promises";
import {
  genericDelete,
  getFilesForJob,
  deleteFileInDb,
  getJobWithStatusAndBeforeDate,
  getLastDumpFileJobDate,
  getSuccessfulJobsBeforeDate,
} from "./queries";
export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  if (!task) return;

  try {
    await updateTaskStatus(task, STATUS_BUSY);
    let lastDumpDate = await getLastDumpFileJobDate();

    let maxDaysToKeepFailedJobs = new Date();
    maxDaysToKeepFailedJobs.setDate(
      maxDaysToKeepFailedJobs.getDate() - MAX_DAYS_TO_KEEP_FAILED_JOBS,
    );
    let maxDaysToKeepBusyJobs = new Date();
    maxDaysToKeepBusyJobs.setDate(
      maxDaysToKeepBusyJobs.getDate() - MAX_DAYS_TO_KEEP_BUSY_JOBS,
    );

    let jobsToClean = [
      ...(await getSuccessfulJobsBeforeDate(lastDumpDate)),
      ...(await getJobWithStatusAndBeforeDate(
        STATUS_FAILED,
        maxDaysToKeepFailedJobs,
      )),
      ...(await getJobWithStatusAndBeforeDate(
        STATUS_BUSY,
        maxDaysToKeepBusyJobs,
      )),
    ];

    console.log(
      `found ${jobsToClean.length} jobs to clean up.`,
    );

    while (jobsToClean.length) {
      const {jobUri, jobId} = jobsToClean.pop();
      await getFilesForJob(jobUri, async (files) => {
        console.log(`found ${files.length} files to clean up for job ${jobUri}`);
        for (let f of files) {
          await removeFile(f);
        }
      });

      console.log(`done cleaning up files`);
      await genericDelete(jobUri);
      await removeJobDirectory(jobId);

      console.log(`job ${jobUri} deleted`);
    }
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

async function removeJobDirectory(jobId) {
  try {
    console.log(`attempting to delete job directory ${jobId}...`);
    const directory = `/share/${jobId}`;
    await rm(directory,{ recursive: true, force: true });
  }catch (e) {
    console.warn(`could not delete job directory ${jobId}`);
  }
}

async function removeFile(file) {
  try {
    console.log(`Deleting ${JSON.stringify(file, null, 2)}`);
    let path = file.fileOnDisk.replace("share://", "/share/");
    await unlink(path);
  } catch (e) {
    console.error(`could not delete ${JSON.stringify(file)}. ${e}`);
  }
  try {
    await deleteFileInDb(file.file);
  } catch (e) {
    console.error(`could not delete file in db: ${JSON.stringify(file)}. ${e}`);
  }
}
