import { appendTaskError, loadExtractionTask, updateTaskStatus } from "./task";
import {
  MAX_DAYS_TO_KEEP_BUSY_JOBS,
  MAX_DAYS_TO_KEEP_FAILED_JOBS,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
} from "../constants";
import { unlink } from "fs/promises";
import {
  genericDelete,
  getFilesForJob,
  deleteFileInDb,
  getJobWithStatusAndBeforeDate,
  getSuccessfulJobsBeforeDate,
} from "./queries";

export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  if (!task) return;

  try {
    await updateTaskStatus(task, STATUS_BUSY);

    // Calculate date thresholds for job cleanup
    const maxDaysToKeepBusyJobs = new Date();
    maxDaysToKeepBusyJobs.setDate(
      maxDaysToKeepBusyJobs.getDate() - MAX_DAYS_TO_KEEP_BUSY_JOBS,
    );

    const maxDaysToKeepFailedJobs = new Date();
    maxDaysToKeepFailedJobs.setDate(
      maxDaysToKeepFailedJobs.getDate() - MAX_DAYS_TO_KEEP_FAILED_JOBS,
    );

    // Get jobs to clean based on:
    // 1. Successful jobs older than MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS
    // 2. Busy jobs older than MAX_DAYS_TO_KEEP_BUSY_JOBS
    // 3. Failed jobs older than MAX_DAYS_TO_KEEP_FAILED_JOBS
    const jobsToClean = [
      ...(await getSuccessfulJobsBeforeDate()),
      ...(await getJobWithStatusAndBeforeDate(
        STATUS_BUSY,
        maxDaysToKeepBusyJobs,
      )),
      ...(await getJobWithStatusAndBeforeDate(
        STATUS_FAILED,
        maxDaysToKeepFailedJobs,
      )),
    ];

    console.log(`Found ${jobsToClean.length} jobs to clean up.`);

    // Clean each job
    for (const jobUri of jobsToClean) {
      console.log(`Starting cleanup for job ${jobUri}`);

      // Delete all files associated with the job (both physical files and DB metadata)
      await getFilesForJob(jobUri, async (files) => {
        console.log(`Found ${files.length} files to clean up for job ${jobUri}`);
        for (const file of files) {
          await removeFile(file);
        }
      });

      console.log(`Done cleaning up files for job ${jobUri}`);

      // Delete the job and all related entities (tasks, containers, etc.)
      await genericDelete(jobUri);
      console.log(`Job ${jobUri} deleted`);
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
