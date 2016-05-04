/*
 * Copyright (c) NASK, NCSC
 * 
 * This file is part of HoneySpider Network 2.1.
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

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import pl.nask.hsn2.DataStoreActiveCleaner.LeaveJobOption;

public class DataStoreCmdLineOptTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreCmdLineOptTest.class);
	private static final String NEGATIVE_ARGUMENT = "-1";
	private static final String INVALID_ARGUMENT = "1a";
	private static final int THREADS_NUMBER = 5;
	private static final int PORT_NUMBER = 1111;
	private static final String EXCHANGE_NAME = "exch";
	private static final String HOST_NAME = "host";
	private static final int DEFAULT_PORT = 8080;
	private static final String DEFAULT_RBT_HOSTNAME = "localhost";
	private static final String DEFAULT_RBT_NOTIFY_EXCH = "notify";
	private static final int DEFAULT_CLEANING_THREADS_NUMBER = 3;

	@Test
	public void allOptions() throws Exception {
		String[] args = { "-ct", String.valueOf(THREADS_NUMBER), "-ld", LeaveJobOption.FAILED.toString().toLowerCase(), "-p",
				String.valueOf(PORT_NUMBER), "-rs", HOST_NAME, "-rne", EXCHANGE_NAME };
		DataStoreCmdLineOptions opt = new DataStoreCmdLineOptions(args);

		Assert.assertEquals(opt.getCleaningThreadsNumber(), THREADS_NUMBER);
		Assert.assertEquals(opt.getPort(), PORT_NUMBER);
		Assert.assertEquals(opt.getRbtHostname(), HOST_NAME);
		Assert.assertEquals(opt.getRbtNotifyExch(), EXCHANGE_NAME);
		Assert.assertEquals(opt.getLeaveData(), LeaveJobOption.FAILED);
	}

	@Test
	public void defaultValues() throws Exception {
		String[] args = {};
		DataStoreCmdLineOptions opt = new DataStoreCmdLineOptions(args);

		Assert.assertEquals(opt.getCleaningThreadsNumber(), DEFAULT_CLEANING_THREADS_NUMBER);
		Assert.assertEquals(opt.getPort(), DEFAULT_PORT);
		Assert.assertEquals(opt.getRbtHostname(), DEFAULT_RBT_HOSTNAME);
		Assert.assertEquals(opt.getRbtNotifyExch(), DEFAULT_RBT_NOTIFY_EXCH);
		Assert.assertEquals(opt.getLeaveData(), LeaveJobOption.NONE);
	}

	@Test
	public void leaveJobData() {
		// Check for '-ld all' option.
		String[] argsLeaveAll = { "-ld", LeaveJobOption.ALL.toString().toLowerCase() };
		try {
			DataStoreCmdLineOptions opt = new DataStoreCmdLineOptions(argsLeaveAll);
			Assert.assertEquals(opt.getLeaveData(), LeaveJobOption.ALL);
		} catch (ParseException e) {
			Assert.fail("There should be no exception here.");
		}

		// Check for '-ld' option with invalid argument.
		String[] argsLeaveInvalid = { "-ld", INVALID_ARGUMENT };
		try {
			new DataStoreCmdLineOptions(argsLeaveInvalid);
			Assert.fail("Should throw an exception at this point.");
		} catch (ParseException e) {
			LOGGER.debug("Exception detected as expected. ({}: {})", e.getClass().getSimpleName(), e.getMessage());
		}
	}

	@Test
	public void helpOption() throws ParseException {
		String[] args = { "-h" };
		DataStoreCmdLineOptions opt = new DataStoreCmdLineOptions(args);

		Assert.assertNull(opt.getRbtHostname());
		Assert.assertNull(opt.getRbtNotifyExch());
	}

	@Test
	public void invalidArguments() throws ParseException {
		// Check for invalid port argument.
		String[] argsPortInvalid = { "-p", INVALID_ARGUMENT };
		try {
			new DataStoreCmdLineOptions(argsPortInvalid);
			Assert.fail("Should throw an exception at this point.");
		} catch (IllegalArgumentException e) {
			LOGGER.debug("Exception detected as expected. ({}: {})", e.getClass().getSimpleName(), e.getMessage());
		}

		// Check for invalid threads number argument.
		String[] argsThreadsNumberInvalid = { "-ct", INVALID_ARGUMENT };
		try {
			new DataStoreCmdLineOptions(argsThreadsNumberInvalid);
			Assert.fail("Should throw an exception at this point.");
		} catch (IllegalArgumentException e) {
			LOGGER.debug("Exception detected as expected. ({}: {})", e.getClass().getSimpleName(), e.getMessage());
		}
	}

	@Test
	public void negativeArguments() throws ParseException {
		// Check for negative port argument.
		String[] argsPortNegative = { "-p", NEGATIVE_ARGUMENT };
		try {
			new DataStoreCmdLineOptions(argsPortNegative);
			Assert.fail("Should throw an exception at this point.");
		} catch (IllegalArgumentException e) {
			LOGGER.debug("Exception detected as expected. ({}: {})", e.getClass().getSimpleName(), e.getMessage());
		}

		// Check for negative threads number argument.
		String[] argsThreadsNumberNegative = { "-ct", NEGATIVE_ARGUMENT };
		try {
			new DataStoreCmdLineOptions(argsThreadsNumberNegative);
			Assert.fail("Should throw an exception at this point.");
		} catch (Exception e) {
			LOGGER.debug("Exception detected as expected. ({}: {})", e.getClass().getSimpleName(), e.getMessage());
		}
	}
}
