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

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pl.nask.hsn2.exceptions.InvalidArgument;

/**
 * Application which can be run from command line and which will delete from
 * Data Store old job files.
 */
public final class DataStoreCleaner {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreCleaner.class);
	public static final String DATA_PATH = DataStore.getDataPath();
	private long jobNumberArgSet = -1;
	private boolean jobArgSet = false;
	private boolean helpArgSet = false;
	private static final String HELP_MSG = "DataStoreCleaner will clean data for all jobs with ID number less\nthan provided in parameters."
			+ "\n\nUsage:\n-h      Show help page.\n-j N    Set current job number. All previous job directories will\n        be cleaned.\n\n";

	private DataStoreCleaner() {
		// this is utility class
	}

	public static void main(String[] args) {
		DataStoreCleaner dsc = new DataStoreCleaner();
		try {
			dsc.parseStartArguments(args);
			dsc.clean();
		} catch (Exception e) {
			LOGGER.info(e.getMessage());
			System.out.println(HELP_MSG); //NOPMD
		}
	}

	private void parseStartArguments(String[] args) throws InvalidArgument {
		if (args.length == 0) {
			helpArgSet = true;
			throw new InvalidArgument("No arguments provided.");
		} else {
			String prevArg = "";
			for (String arg : args) {
				if (arg.equals("-h")) {
					// Help wanted.
					helpArgSet = true;
				} else if (arg.equals("-j")) {
					// Jobs argument provided.
					jobArgSet = true;
				} else {
					if (prevArg.equals("-j")) {
						// Number expected.
						jobNumberArgSet = Long.parseLong(arg);
						if (jobNumberArgSet < 1) {
							throw new InvalidArgument("Job number should be positive and it is: " + jobNumberArgSet);
						}
					} else {
						// Unknown argument.
						throw new InvalidArgument("Invalid argument: " + arg);
					}
				}
				prevArg = arg;
			}
		}
		if (jobArgSet) {
			if (jobNumberArgSet == -1) {
				throw new InvalidArgument("Job number missing or -1 provided (should be positive number).");
			} else {
				LOGGER.info("Job numer has been set to: {}", jobNumberArgSet);
			}
		}
		if (helpArgSet) {
			System.out.println(HELP_MSG); //NOPMD
		}
	}

	private void clean() {
		if (jobArgSet && jobNumberArgSet > 0) {
			LOGGER.info("Cleaning started.");
			File dataDir = new File(DATA_PATH);
			File[] files = dataDir.listFiles();
			for (File f : files) {
				if (f.isDirectory()) {
					try {
						String name = f.getName();
						long id = Long.parseLong(name);
						if (id < jobNumberArgSet) {
							deleteNonEmptyDirectory(f);
						}
					} catch (NumberFormatException e) {
						LOGGER.info("Directory/File is not number, leave it untouched: {}", f.getAbsolutePath());
					}
				}
			}
			LOGGER.info("Cleaning ended.");
		}
	}

	public static void deleteNonEmptyDirectory(File dir) {
		LOGGER.debug("Non empty file/dir deleting: {}", dir.getAbsolutePath());
		if (dir.isDirectory()) {
			File[] filesInside = dir.listFiles();
			LOGGER.debug("{} is directory and has {} items inside.", dir.getAbsolutePath(), filesInside.length);
			for (File file : filesInside) {
				deleteNonEmptyDirectory(file);
			}
		}
		if (dir.delete()) {
			LOGGER.debug("Deleted: {}", dir.getAbsolutePath());
		} else {
			LOGGER.info("Cannot delete: {}", dir.getAbsolutePath());
		}
	}
}
