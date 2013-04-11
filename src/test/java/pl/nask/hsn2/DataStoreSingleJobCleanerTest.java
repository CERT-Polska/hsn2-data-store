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

import java.io.File;
import java.io.IOException;
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
		long jobIdToClean = 1;
		ConcurrentSkipListSet<Long> actualCleaningJobsList = createTestFiles(jobIdToClean);

		// Do test.
		DataStoreCleanSingleJob cleaner = new DataStoreCleanSingleJob(actualCleaningJobsList, jobIdToClean);
		Thread thread = new Thread(cleaner);
		thread.start();
		thread.join();

		// Asserts.
		Assert.assertTrue(actualCleaningJobsList.isEmpty(), "Cleaning job list should be empty.");
		assertH2DbFiles(jobIdToClean);
	}

	private void assertH2DbFiles(long jobIdToClean) {
		Path path;
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".h2.db").toPath();
		Assert.assertTrue(Files.notExists(path), "H2 db file should be removed, but it exists. " + path);
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".lock.db").toPath();
		Assert.assertTrue(Files.notExists(path), "H2 db lock file should be removed, but it exists. " + path);
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".trace.db").toPath();
		Assert.assertTrue(Files.notExists(path), "H2 db trace file should be removed, but it exists. " + path);
	}

	private ConcurrentSkipListSet<Long> createTestFiles(long jobIdToClean) throws IOException {
		// Make sure data dir exists.
		File dataFile = new File(DataStore.getDbFileName(jobIdToClean)).getParentFile();
		if (!dataFile.exists()) {
			Files.createDirectories(dataFile.toPath());
		}

		// Create files.
		Path path;
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".h2.db").toPath();
		Files.createFile(path);
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".lock.db").toPath();
		Files.createFile(path);
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".trace.db").toPath();
		Files.createFile(path);
		ConcurrentSkipListSet<Long> actualCleaningJobsList = new ConcurrentSkipListSet<>();
		actualCleaningJobsList.add(jobIdToClean);
		return actualCleaningJobsList;
	}
}
