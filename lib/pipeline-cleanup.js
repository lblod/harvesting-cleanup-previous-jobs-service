import { appendTaskError, loadExtractionTask, updateTaskStatus } from './task';
import {
  MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS,
  MAX_DAYS_TO_KEEP_BUSY_JOBS,
  MAX_DAYS_TO_KEEP_FAILED_JOBS,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
} from '../constants';
import { unlink } from 'fs/promises';
import {
  genericDelete,
  getFilesForJob,
  deleteFileInDb,
  getJobWithStatusAndBeforeDate,
} from './queries';

export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  if (!task) return;

  try {
    await updateTaskStatus(task, STATUS_BUSY);

    // Calculate date thresholds for job cleanup

    const maxDaysToKeepSuccessfulJobs = new Date();
    maxDaysToKeepSuccessfulJobs.setDate(
      maxDaysToKeepSuccessfulJobs.getDate() - MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS,
    );

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
      ...(await getJobWithStatusAndBeforeDate(
        STATUS_SUCCESS,
        maxDaysToKeepSuccessfulJobs,
      )),
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
        console.log(
          `Found ${files.length} files to clean up for job ${jobUri}`,
        );
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
  console.log(`Deleting ${JSON.stringify(file, null, 2)}`);

  let physicalFileDeleted = false;
  let dbFileDeleted = false;
  let physicalFileError = null;
  let dbFileError = null;

  // Try to delete the physical file
  try {
    let path = file.fileOnDisk.replace('share://', '/share/');
    await unlink(path);
    physicalFileDeleted = true;
    console.log(`Physical file deleted: ${path}`);
  } catch (e) {
    physicalFileError = e;
    // Only log as warning if file doesn't exist (already deleted), otherwise it's an error
    if (e.code === 'ENOENT') {
      console.warn(
        `Physical file not found (may already be deleted): ${file.fileOnDisk}`,
      );
      physicalFileDeleted = true; // Consider it deleted if it doesn't exist
    } else {
      console.error(
        `Failed to delete physical file ${file.fileOnDisk}: ${e.message}`,
      );
    }
  }

  // Try to delete the file metadata from database
  try {
    await deleteFileInDb(file.file);
    dbFileDeleted = true;
    console.log(`Database entry deleted: ${file.file}`);
  } catch (e) {
    dbFileError = e;
    console.error(
      `Failed to delete file in database: ${file.file}. ${e.message}`,
    );
  }

  // Report overall status
  if (!physicalFileDeleted || !dbFileDeleted) {
    const errors = [];
    if (!physicalFileDeleted)
      errors.push(`physical file: ${physicalFileError?.message}`);
    if (!dbFileDeleted) errors.push(`database: ${dbFileError?.message}`);
    throw new Error(
      `Failed to fully delete file ${file.file}. Errors: ${errors.join(', ')}`,
    );
  }
}
