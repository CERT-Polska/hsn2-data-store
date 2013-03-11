/*
 * Copyright (c) NASK, NCSC
 * 
 * This file is part of HoneySpider Network 2.0.
 * 
 * This is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
