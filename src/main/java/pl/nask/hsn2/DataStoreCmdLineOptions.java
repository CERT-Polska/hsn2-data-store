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

	private static final int DEFAULT_PORT = 8080;
	private static final String DEFAULT_RBT_HOSTNAME = "localhost";
	private static final String DEFAULT_RBT_NOTIFY_EXCH = "notify";
	private static final int DEFAULT_CLEANING_THREADS_NUMBER = 3;

	private int port;
	private String rbtHostname;
	private String rbtNotifyExch;
	private LeaveJobOption leaveData;
	private int cleaningThreadsNumber;

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

	/**
	 * Parse arguments and set options.
	 * 
	 * @param args
	 * @throws ParseException
	 */
	private void parseParams(String[] args) throws ParseException {
		// Parse arguments.
		CommandLine cmd = new PosixParser().parse(options, args);

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

	public int getPort() {
		return port;
	}

	public String getRbtHostname() {
		return rbtHostname;
	}

	public String getRbtNotifyExch() {
		return rbtNotifyExch;
	}

	public LeaveJobOption getLeaveData() {
		return leaveData;
	}

	public int getCleaningThreadsNumber() {
		return cleaningThreadsNumber;
	}
}
