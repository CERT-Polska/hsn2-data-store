package pl.nask.hsn2;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataStoreSingleJobCleanerTest {
	static final Logger LOGGER = LoggerFactory.getLogger(DataStoreSingleJobCleanerTest.class);

	@Test
	public void jobDataCleaning() throws Exception {
		// Init
		ConcurrentSkipListSet<Long> actualCleaningJobsList = new ConcurrentSkipListSet<>();
		long jobIdToClean = 1;
		actualCleaningJobsList.add(jobIdToClean);
		Path dataDirToClean = DataStoreTestUtils.prepareTempDir();
		DataStoreCleanSingleJob cleaner = new DataStoreCleanSingleJob(actualCleaningJobsList, jobIdToClean, dataDirToClean.toFile());
		Thread thread = new Thread(cleaner);
		thread.start();
		thread.join();

		Assert.assertTrue(actualCleaningJobsList.isEmpty(), "Cleaning job list should be empty.");
		Assert.assertTrue(Files.notExists(dataDirToClean), "Temp dir should be removed, but it exists. " + dataDirToClean);
	}
}
