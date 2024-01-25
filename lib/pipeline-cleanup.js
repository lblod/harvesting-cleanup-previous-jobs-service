import { appendTaskError, loadExtractionTask, updateTaskStatus } from "./task";
import { STATUS_BUSY, STATUS_FAILED, STATUS_SUCCESS } from "../constant";
import { unlink } from "fs/promises";
import {
  countFileForJob,
  genericDelete,
  getFilesForJob,
  getLastDumpFileJobDate,
  getSuccessfulJobsBeforeDate,
} from "./queries";
export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  if (!task) return;

  try {
    await updateTaskStatus(task, STATUS_BUSY);
    let lastDumpDate = await getLastDumpFileJobDate();
    let successfulJobsToClean = await getSuccessfulJobsBeforeDate(lastDumpDate);
    while (successfulJobsToClean.length) {
      const jobUri = successfulJobsToClean.pop();
      const nbFiles = await countFileForJob(jobUri);
      console.log(`cleaning job ${jobUri} with ${nbFiles} files...`);
      const files = await getFilesForJob(jobUri, nbFiles);
      await Promise.all(files.map(async (f) => await removeFile(f)));
      console.log(`done cleaning up files`);
      await genericDelete(jobUri);
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

async function removeFile(file) {
  try {
    let path = file.fileOnDisk.replace("share://", "/share/");
    await unlink(path);
    await genericDelete(file.file);
  } catch (e) {
    console.error(`could not delete ${JSON.stringify(file)}. ${e}`);
  }
}
