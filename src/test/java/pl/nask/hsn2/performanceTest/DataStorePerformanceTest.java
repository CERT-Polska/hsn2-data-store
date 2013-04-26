/*
 * Copyright (c) NASK, NCSC
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

package pl.nask.hsn2.performanceTest;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pl.nask.hsn2.ResourceException;
import pl.nask.hsn2.StorageException;
import pl.nask.hsn2.bus.api.Destination;
import pl.nask.hsn2.bus.api.Message;
import pl.nask.hsn2.bus.operations.JobFinished;
import pl.nask.hsn2.bus.operations.JobStatus;
import pl.nask.hsn2.bus.operations.Operation;
import pl.nask.hsn2.bus.rabbitmq.RbtDestination;
import pl.nask.hsn2.bus.serializer.MessageSerializer;
import pl.nask.hsn2.bus.serializer.MessageSerializerException;
import pl.nask.hsn2.bus.serializer.protobuf.ProtoBufMessageSerializer;
import pl.nask.hsn2.connector.REST.DataResponse;
import pl.nask.hsn2.connector.REST.DataStoreConnector;
import pl.nask.hsn2.connector.REST.DataStoreConnectorImpl;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class DataStorePerformanceTest {
	private static final String DEFAULT_CONTENT_TYPE = "application/hsn2+protobuf";
	private static final String TEST_TEXT = "This is long text. This is long text. This is long text. ";
	private static final int TEST_TEXT_LENGTH = TEST_TEXT.length();
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStorePerformanceTest.class);

	private static Path bigFile;
	private static Path smallFile;
	private static MessageSerializer<Operation> defaultSerializer;
	private static ExecutorService executor;
	private static long jobId;
	private static DataStoreConnector dsConnector;
	private static String rbtHostName;
	private static String rbtNotifyExchName;
	private static String rbtMainExchName;

	private static void createTempFile(Path file, int fileSize) throws IOException {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(file.toFile()))) {
			int size = 0;
			while (size < fileSize) {
				bw.write(TEST_TEXT);
				size += TEST_TEXT_LENGTH;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// Init.
		CmdLineOpt cmdLineOpt = new CmdLineOpt(args);
		prepareTempFiles(cmdLineOpt);
		prepareStaticVariables(cmdLineOpt);
		LOGGER.info("\nStarting with options:\n- files to test = {}\n- threads number = {}\n- job id = {}\n"
				+ "- big file size = {}\n- small file size = {}\n- small files per iteration = {}\n"
				+ "- big files per iteration = {}\n- rabbitmq host = {}\n- rabbitmq main exch = {}\n- rabbitmq notify"
				+ " exch = {}\n- data store url = {}\n", cmdLineOpt.getHowManyFiles(), cmdLineOpt.getThreadsNumber(), jobId,
				cmdLineOpt.getBigFileSize(), cmdLineOpt.getSmallFileSize(), cmdLineOpt.getSmallFilesLimit(), cmdLineOpt.getBigFilesLimit(),
				rbtHostName, rbtMainExchName, rbtNotifyExchName, cmdLineOpt.getDsUrl());

		// Submit all tasks.
		CountDownLatch latch = new CountDownLatch(2 * cmdLineOpt.getHowManyFiles());
		ActionType nextAction = ActionType.SMALL_FILE;
		int filesCounter = 0;
		long time = System.currentTimeMillis();
		for (int i = 0; i < cmdLineOpt.getHowManyFiles(); i++) {
			// Submit add data task. (Which will run receive data task when finished.)
			executor.execute(new AddDataTask(nextAction, latch));
			filesCounter++;
			if (filesCounter == nextAction.getFileLimit()) {
				filesCounter = 0;
				nextAction = nextAction.next();
			}
		}

		// Wait for all tasks to complete.
		try {
			latch.await();
		} catch (InterruptedException e) {
			LOGGER.error("Waiting for job done interrupted.", e);
		}

		// Log duration time.
		time = System.currentTimeMillis() - time;
		LOGGER.info("Files created. Time = {} sec", time / 1000d);

		// Run job cleaning task.
		executor.execute(new DeleteJobDataTask());

		// Cleaning.
		Files.delete(smallFile);
		Files.delete(bigFile);
		executor.shutdown();
	}

	private static void prepareStaticVariables(CmdLineOpt cmdLineOpt) {
		defaultSerializer = new ProtoBufMessageSerializer();
		executor = Executors.newFixedThreadPool(cmdLineOpt.getThreadsNumber());
		jobId = cmdLineOpt.getJobId();
		dsConnector = new DataStoreConnectorImpl(cmdLineOpt.getDsUrl());
		rbtHostName = cmdLineOpt.getRbtHost();
		rbtMainExchName = cmdLineOpt.getRbtMainExch();
		rbtNotifyExchName = cmdLineOpt.getRbtNotifyExch();
	}

	private static void prepareTempFiles(CmdLineOpt cmdLnOpt) {
		// Prepare test String.
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			sb.append(TEST_TEXT);
		}
		sb.toString();

		try {
			// Prepare big file.
			bigFile = Files.createTempFile("hsn2-data-store-perfTest-", "");
			createTempFile(bigFile, cmdLnOpt.getBigFileSize());

			// Prepare small file.
			smallFile = Files.createTempFile("hsn2-data-store-perfTest-", "");
			createTempFile(smallFile, cmdLnOpt.getSmallFileSize());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		// Set files limits.
		ActionType.SMALL_FILE.setFilesLimit(cmdLnOpt.getSmallFilesLimit());
		ActionType.BIG_FILE.setFilesLimit(cmdLnOpt.getBigFilesLimit());
	}

	private static enum ActionType {
		SMALL_FILE {
			@Override
			public ActionType next() {
				return BIG_FILE;
			}
		},
		BIG_FILE {
			@Override
			public ActionType next() {
				return SMALL_FILE;
			}
		};

		private int filesLimit = 1;

		public abstract ActionType next();

		private void setFilesLimit(int filesLimitCounter) {
			filesLimit = filesLimitCounter;
		}

		public int getFileLimit() {
			return filesLimit;
		}
	}

	private static class AddDataTask implements Runnable {
		private final ActionType actionToDo;
		private final CountDownLatch latch;

		public AddDataTask(ActionType action, CountDownLatch countdownLatch) {
			actionToDo = action;
			latch = countdownLatch;
		}

		@Override
		public void run() {
			DataResponse dr;
			long key = 0;
			boolean isSuccess = false;
			try {
				switch (actionToDo) {
				case BIG_FILE:
					LOGGER.debug("Adding big file...");
					try (InputStream bigFileIs = new BufferedInputStream(new FileInputStream(bigFile.toFile()))) {
						dr = dsConnector.sendPost(bigFileIs, jobId);
						key = dr.getKeyId();
						isSuccess = dr.isSuccesful();
					}
					break;
				case SMALL_FILE:
					LOGGER.debug("Adding small file...");
					try (InputStream smallFileIs = new BufferedInputStream(new FileInputStream(smallFile.toFile()))) {
						dr = dsConnector.sendPost(smallFileIs, jobId);
						key = dr.getKeyId();
						isSuccess = dr.isSuccesful();
					}
					break;
				}
				LOGGER.debug("Data added. Response(type={}, key={})", isSuccess, key);
				executor.execute(new GetDataTask(key, latch));
				latch.countDown();
			} catch (IOException e) {
				LOGGER.info("Could not complete SEND task (send string).", e);
				latch.countDown();
			}
		}
	}

	private static class GetDataTask implements Runnable {
		private final long referenceId;
		private final CountDownLatch latch;

		public GetDataTask(long refId, CountDownLatch countDownLatch) {
			referenceId = refId;
			latch = countDownLatch;
		}

		@Override
		public void run() {
			try {
				try (BufferedInputStream bis = new BufferedInputStream(dsConnector.getResourceAsStream(jobId, referenceId))) {
					StringBuilder sb = new StringBuilder();
					int chInt;
					int size = 0;
					while ((chInt = bis.read()) != -1) {
						sb.append((char) chInt);
						size++;
					}
					LOGGER.debug("Data received. Stream size = {}", size);
				}
				latch.countDown();
			} catch (IOException | ResourceException | StorageException e) {
				LOGGER.info("Could not complete GET task (send string).", e);
				latch.countDown();
			}
		}
	}

	private static class DeleteJobDataTask implements Runnable {
		@Override
		public void run() {
			try {
				iniRabbitMqAndSendJobFinished();
				LOGGER.info("JobFinished message sent.");
			} catch (IOException e) {
				LOGGER.info("JobFinished message not sent.", e);
			}
		}

		private void iniRabbitMqAndSendJobFinished() throws IOException {
			Connection rbtConnection;
			ConnectionFactory connFactory = new ConnectionFactory();
			connFactory.setHost(rbtHostName);
			rbtConnection = connFactory.newConnection();
			Channel channel = rbtConnection.createChannel();
			channel.exchangeDeclare(rbtNotifyExchName, "fanout");
			Operation jobFinishedOperation = new JobFinished(jobId, JobStatus.COMPLETED);
			Destination dst = new RbtDestination(rbtMainExchName, "");
			sendOut(jobFinishedOperation, channel, dst, "");
			rbtConnection.close();
		}

		private void sendOut(Operation operation, Channel channel, Destination dest, String replyTo) throws IOException {
			try {
				// Prepare message.
				Message message = defaultSerializer.serialize(operation);
				message.setDestination(dest);
				message.setCorrelationId(null);
				message.setReplyTo(new RbtDestination(rbtMainExchName, replyTo));

				// Properties below uses only service name (String, not Destination) as replyTo parameter.
				String replyToQueueName = message.getReplyTo().getService();

				HashMap<String, Object> headers = new HashMap<String, Object>();
				BasicProperties.Builder propertiesBuilder = new BasicProperties.Builder().headers(headers)
						.contentType(DEFAULT_CONTENT_TYPE).replyTo(replyToQueueName).type(message.getType());

				// setup correct correlation id if provided
				if (message.getCorrelationId() != null && !"".equals(message.getCorrelationId())) {
					propertiesBuilder.correlationId(message.getCorrelationId());
				}
				String destinationRoutingKey = ((RbtDestination) (message.getDestination())).getService();
				String destinationExchange = ((RbtDestination) (message.getDestination())).getExchange();
				channel.basicPublish(destinationExchange, destinationRoutingKey, propertiesBuilder.build(), message.getBody());
			} catch (MessageSerializerException e) {
				LOGGER.error("Serialization error.", e);
			}
		}
	}

	private static class CmdLineOpt {
		private static final int CONSOLE_WIDTH = 110;
		private final Options options = new Options();

		private static final String HOW_MANY_FILES_DEFAULT = "10";
		private static final String JOB_ID_DEFAULT = "123456789";
		private static final String THREADS_NUMBER_DEFAULT = "10";
		private static final String SMALL_FILE_SIZE_DEFAULT = "8000000";
		private static final String BIG_FILE_SIZE_DEFAULT = "20000000";
		private static final String SMALL_FILES_LIMIT_DEFAULT = "10";
		private static final String BIG_FILES_LIMIT_DEFAULT = "1";
		private static final String DS_URL_DEFAULT = "http://127.0.0.1:8080/";
		private static final String RABBITMQ_HOST_DEFAULT = "localhost";
		private static final String RABBITMQ_NOTIFY_EXCH_DEFAULT = "notify";
		private static final String RABBITMQ_MAIN_EXCH_DEFAULT = "main";

		private int howManyFiles;
		private long jobId;
		private int threadsNumber;
		private int smallFileSize;
		private int bigFileSize;
		private int smallFilesLimit;
		private int bigFilesLimit;
		private String dsUrl;
		private String rbtHost;
		private String rbtNotifyExch;
		private String rbtMainExch;

		public CmdLineOpt(String[] args) throws ParseException {
			initOptions();
			parseCmdLineArgs(args);
		}

		private void initOptions() {
			OptionBuilder.withDescription("Prints this help page.");
			OptionBuilder.withLongOpt("help");
			options.addOption(OptionBuilder.create("h"));

			OptionBuilder.withDescription("How many files to test. One PUT and one GET action for each file. (Default: "
					+ HOW_MANY_FILES_DEFAULT + ")");
			OptionBuilder.withLongOpt("files");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("f"));

			OptionBuilder.withDescription("Job id. (Default: " + JOB_ID_DEFAULT + ")");
			OptionBuilder.withLongOpt("job");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("j"));

			OptionBuilder.withDescription("Threads number. (Default: " + THREADS_NUMBER_DEFAULT + ")");
			OptionBuilder.withLongOpt("threads");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("t"));

			OptionBuilder.withDescription("Small file size in bytes. (Default: " + SMALL_FILE_SIZE_DEFAULT + ")");
			OptionBuilder.withLongOpt("smallSize");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("ss"));

			OptionBuilder.withDescription("Big file size in bytes. (Default: " + BIG_FILE_SIZE_DEFAULT + ")");
			OptionBuilder.withLongOpt("bigSize");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("bs"));

			OptionBuilder.withDescription("Small files number per iteration. (Default: " + SMALL_FILES_LIMIT_DEFAULT + ")");
			OptionBuilder.withLongOpt("smlFilesPerIter");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("sfpi"));

			OptionBuilder.withDescription("Big files number per iteration. (Default: " + BIG_FILES_LIMIT_DEFAULT + ")");
			OptionBuilder.withLongOpt("bigFilesPerIter");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("bfpi"));

			OptionBuilder.withDescription("RabbitMQ notify exchange. (Default: " + RABBITMQ_NOTIFY_EXCH_DEFAULT + ")");
			OptionBuilder.withLongOpt("rbtNtfExch");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("rne"));

			OptionBuilder.withDescription("RabbitMQ main exchange. (Default: " + RABBITMQ_MAIN_EXCH_DEFAULT + ")");
			OptionBuilder.withLongOpt("rbtMainExch");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("rme"));

			OptionBuilder.withDescription("RabbitMQ host name. (Default: " + RABBITMQ_HOST_DEFAULT + ")");
			OptionBuilder.withLongOpt("rbtHost");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("rh"));

			OptionBuilder.withDescription("Data store URL. (Default: " + DS_URL_DEFAULT + ")");
			OptionBuilder.withLongOpt("dsUrl");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("du"));
		}

		private void parseCmdLineArgs(String[] args) throws ParseException {
			// Parse arguments.
			CommandLine cmd = new PosixParser().parse(options, args);

			// Check for options.
			if (cmd.hasOption("help")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.setWidth(CONSOLE_WIDTH);
				formatter.printHelp("java -jar ...", options);
				System.exit(0);
			} else {
				howManyFiles = Integer.valueOf(cmd.getOptionValue("f", HOW_MANY_FILES_DEFAULT));
				jobId = Long.valueOf(cmd.getOptionValue("j", JOB_ID_DEFAULT));
				threadsNumber = Integer.valueOf(cmd.getOptionValue("t", THREADS_NUMBER_DEFAULT));
				smallFileSize = Integer.valueOf(cmd.getOptionValue("ss", SMALL_FILE_SIZE_DEFAULT));
				bigFileSize = Integer.valueOf(cmd.getOptionValue("bs", BIG_FILE_SIZE_DEFAULT));
				smallFilesLimit = Integer.valueOf(cmd.getOptionValue("sfpi", SMALL_FILES_LIMIT_DEFAULT));
				bigFilesLimit = Integer.valueOf(cmd.getOptionValue("bfpi", BIG_FILES_LIMIT_DEFAULT));
				rbtHost = cmd.getOptionValue("rh", RABBITMQ_HOST_DEFAULT);
				rbtNotifyExch = cmd.getOptionValue("rne", RABBITMQ_NOTIFY_EXCH_DEFAULT);
				rbtMainExch = cmd.getOptionValue("rme", RABBITMQ_MAIN_EXCH_DEFAULT);
				dsUrl = cmd.getOptionValue("du", DS_URL_DEFAULT);
			}
		}

		public int getHowManyFiles() {
			return howManyFiles;
		}

		public long getJobId() {
			return jobId;
		}

		public int getThreadsNumber() {
			return threadsNumber;
		}

		public int getSmallFileSize() {
			return smallFileSize;
		}

		public int getBigFileSize() {
			return bigFileSize;
		}

		public int getSmallFilesLimit() {
			return smallFilesLimit;
		}

		public int getBigFilesLimit() {
			return bigFilesLimit;
		}

		public String getDsUrl() {
			return dsUrl;
		}

		public String getRbtHost() {
			return rbtHost;
		}

		public String getRbtNotifyExch() {
			return rbtNotifyExch;
		}

		public String getRbtMainExch() {
			return rbtMainExch;
		}
	}
}
