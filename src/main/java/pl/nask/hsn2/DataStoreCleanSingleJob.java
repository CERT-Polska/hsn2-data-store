package pl.nask.hsn2;

import java.io.File;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStoreCleanSingleJob implements Runnable {
	private static final double ONE_MIN_IN_MS = 1000d;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreCleanSingleJob.class);
	private final ConcurrentSkipListSet<Long> currentlyCleaningJobs;
	private final long jobId;
	private final File dirToClean;

	public DataStoreCleanSingleJob(ConcurrentSkipListSet<Long> actualCleaningJobsList, long jobIdToClean, File dataDirToClean) {
		currentlyCleaningJobs = actualCleaningJobsList;
		jobId = jobIdToClean;
		currentlyCleaningJobs.add(jobId);
		dirToClean = dataDirToClean;
		LOGGER.debug("Single cleaner initialized. (job={})", jobIdToClean);
	}

	@Override
	public void run() {
		LOGGER.debug("Single cleaner task started. (job={})", jobId);
		long time = System.currentTimeMillis();

		// Clean.
		DataStoreCleaner.deleteNonEmptyDirectory(dirToClean);

		// Task ended. Remove job from actual cleaning jobs list.
		currentlyCleaningJobs.remove(jobId);

		time = System.currentTimeMillis() - time;
		LOGGER.info("Single cleaner task finished. (job={}, time[sec]={})", jobId, time / ONE_MIN_IN_MS);
	}
}
