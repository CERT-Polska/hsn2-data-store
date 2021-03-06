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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import pl.nask.hsn2.DataStoreActiveCleaner.LeaveJobOption;

public class DataStoreCmdLineOptions {
	private static final int CONSOLE_WIDTH = 110;

	private final Options options = new Options();

	private static final String DEFAULT_LOG_LEVEL = "DEBUG";
	private static final int DEFAULT_PORT = 8080;
	private static final String DEFAULT_RBT_HOSTNAME = "localhost";
	private static final String DEFAULT_RBT_NOTIFY_EXCH = "notify";
	private static final int DEFAULT_CLEANING_THREADS_NUMBER = 3;

	private int port;
	private String rbtHostname;
	private String rbtNotifyExch;
	private LeaveJobOption leaveData;
	private int cleaningThreadsNumber;

	private CommandLine cmd;

	public DataStoreCmdLineOptions(String[] args) throws ParseException {
		initOptions();
		parseParams(args);
	}

	/**
	 * Validation rules initialization.
	 */
	private void initOptions() {
		OptionBuilder.withDescription("Prints this help page.");
		OptionBuilder.withLongOpt("help");
		options.addOption(OptionBuilder.create("h"));

		OptionBuilder.withDescription("use given level for log. (Default: " + DEFAULT_LOG_LEVEL + ")");
		OptionBuilder.withLongOpt("logLevel");
		OptionBuilder.hasArgs(1);
		OptionBuilder.withArgName("level");
		options.addOption(OptionBuilder.create("ll"));

		OptionBuilder.withDescription("Listening port. (Default: " + DEFAULT_PORT + ")");
		OptionBuilder.withLongOpt("port");
		OptionBuilder.hasArgs(1);
		OptionBuilder.withArgName("number");
		options.addOption(OptionBuilder.create("p"));

		OptionBuilder.withDescription("Do not remove job data after job ends. If not set, all files are removed.");
		OptionBuilder.withLongOpt("leaveData");
		OptionBuilder.hasArgs(1);
		OptionBuilder.withArgName("filter[failed|all]");
		options.addOption(OptionBuilder.create("ld"));

		OptionBuilder.withDescription("RabbitMQ server hostname. (Default: " + DEFAULT_RBT_HOSTNAME + ")");
		OptionBuilder.withLongOpt("rbtServer");
		OptionBuilder.hasArgs(1);
		OptionBuilder.withArgName("hostname");
		options.addOption(OptionBuilder.create("rs"));

		OptionBuilder.withDescription("RabbitMQ notification exchange name. (Default: " + DEFAULT_RBT_NOTIFY_EXCH + ")");
		OptionBuilder.withLongOpt("rbtNotifyExchange");
		OptionBuilder.hasArgs(1);
		OptionBuilder.withArgName("name");
		options.addOption(OptionBuilder.create("rne"));

		OptionBuilder.withDescription("Cleaning threads number. (Default: " + DEFAULT_CLEANING_THREADS_NUMBER + ")");
		OptionBuilder.withLongOpt("cleaningThreads");
		OptionBuilder.hasArgs(1);
		OptionBuilder.withArgName("number");
		options.addOption(OptionBuilder.create("ct"));
	}

	public final CommandLine getCmd() {
		return cmd;
	}

	public final void setCmd(CommandLine cmd) {
		this.cmd = cmd;
	}

	/**
	 * Parse arguments and set options.
	 *
	 * @param args
	 * @throws ParseException
	 */
	private void parseParams(String[] args) throws ParseException {
		// Parse arguments.
		setCmd(new PosixParser().parse(options, args));

		// Check for options.
		if (cmd.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(CONSOLE_WIDTH);
			formatter.printHelp("java -jar ...", options);
		} else {
			setPortOption(cmd);
			setRabbitMqServerName(cmd);
			setRabbitMqNotifyExchange(cmd);
			setLeaveDataOption(cmd);
			setCleaningThreadsNumber(cmd);
		}
	}

	private void setCleaningThreadsNumber(CommandLine cmd) {
		if (cmd.hasOption("ct")) {
			try {
				cleaningThreadsNumber = Integer.parseInt(cmd.getOptionValue("ct"));
				if (cleaningThreadsNumber < 1) {
					throw new NumberFormatException("Negative '-ct' value. Got: " + cmd.getOptionValue("ct"));
				}
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Illegal '-ct' argument. Got: " + cmd.getOptionValue("ct"), e);
			}
		} else {
			cleaningThreadsNumber = DEFAULT_CLEANING_THREADS_NUMBER;
		}
	}

	private void setLeaveDataOption(CommandLine cmd) throws ParseException {
		if (cmd.hasOption("ld")) {
			String temp = cmd.getOptionValue("ld");
			if ("failed".equals(temp)) {
				leaveData = LeaveJobOption.FAILED;
			} else if ("all".equals(temp)) {
				leaveData = LeaveJobOption.ALL;
			} else {
				throw new ParseException("Only 'filter' and 'all' arguments allowed for '-ld' option. Got: " + temp);
			}
		} else {
			leaveData = LeaveJobOption.NONE;
		}
	}

	private void setRabbitMqNotifyExchange(CommandLine cmd) {
		if (cmd.hasOption("rne")) {
			rbtNotifyExch = cmd.getOptionValue("rne");
		} else {
			rbtNotifyExch = DEFAULT_RBT_NOTIFY_EXCH;
		}
	}

	private void setRabbitMqServerName(CommandLine cmd) {
		if (cmd.hasOption("rs")) {
			rbtHostname = cmd.getOptionValue("rs");
		} else {
			rbtHostname = DEFAULT_RBT_HOSTNAME;
		}
	}

	private void setPortOption(CommandLine cmd) {
		if (cmd.hasOption("p")) {
			try {
				port = Integer.parseInt(cmd.getOptionValue("p"));
				if (port < 1) {
					throw new NumberFormatException("Negative '-p' value. Got: " + cmd.getOptionValue("p"));
				}
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Illegal '-p' argument. Got: " + cmd.getOptionValue("p"), e);
			}
		} else {
			port = DEFAULT_PORT;
		}
	}

	public final int getPort() {
		return port;
	}

	public final String getRbtHostname() {
		return rbtHostname;
	}

	public final String getRbtNotifyExch() {
		return rbtNotifyExch;
	}

	public final LeaveJobOption getLeaveData() {
		return leaveData;
	}

	public final int getCleaningThreadsNumber() {
		return cleaningThreadsNumber;
	}
}
