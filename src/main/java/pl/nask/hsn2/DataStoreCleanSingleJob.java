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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStoreCleanSingleJob implements Runnable {
	private static final double ONE_MIN_IN_MS = 1000d;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreCleanSingleJob.class);
	private final ConcurrentSkipListSet<Long> currentlyCleaningJobs;
	private final long jobId;
	private final Connection h2Connection;
	private final ConcurrentSkipListSet<Long> activeJobs;

	public DataStoreCleanSingleJob(ConcurrentSkipListSet<Long> actualCleaningJobsList, long jobIdToClean, Connection h2dbConnection,
			ConcurrentSkipListSet<Long> activeJobsSet) {
		activeJobs = activeJobsSet;
		h2Connection = h2dbConnection;
		currentlyCleaningJobs = actualCleaningJobsList;
		jobId = jobIdToClean;
		currentlyCleaningJobs.add(jobId);
		LOGGER.debug("Single cleaner initialized. (job={})", jobIdToClean);
	}

	@Override
	public void run() {
		LOGGER.debug("Single cleaner task started. (job={})", jobId);
		long time = System.currentTimeMillis();

		// Clean.
		try {
			dropJobTable();
		} catch (SQLException e) {
			LOGGER.error("Drop database, failed.", e);
		}

		// Task ended. Remove job from actual cleaning jobs list.
		currentlyCleaningJobs.remove(jobId);
		activeJobs.remove(jobId);

		time = System.currentTimeMillis() - time;
		LOGGER.info("Single cleaner task finished. (job={}, time[sec]={})", jobId, time / ONE_MIN_IN_MS);
	}

	private void dropJobTable() throws SQLException {
		String sqlQuery = "DROP TABLE JOB_" + jobId;
		try (PreparedStatement statement = h2Connection.prepareStatement(sqlQuery)) {
			statement.execute();
			LOGGER.info("Table delete - done.");
		}
	}
}
