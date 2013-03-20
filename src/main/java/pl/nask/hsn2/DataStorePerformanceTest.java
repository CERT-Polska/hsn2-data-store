package pl.nask.hsn2;

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

import pl.nask.hsn2.bus.api.Destination;
import pl.nask.hsn2.bus.api.Message;
import pl.nask.hsn2.bus.operations.JobFinished;
import pl.nask.hsn2.bus.operations.JobStatus;
import pl.nask.hsn2.bus.operations.Operation;
import pl.nask.hsn2.bus.rabbitmq.RbtDestination;
import pl.nask.hsn2.bus.serializer.MessageSerializer;
import pl.nask.hsn2.bus.serializer.MessageSerializerException;
import pl.nask.hsn2.bus.serializer.protobuf.ProtoBufMessageSerializer;
import pl.nask.hsn2.connector.REST.DataStoreConnector;
import pl.nask.hsn2.connector.REST.DataStoreConnectorImpl;
import pl.nask.hsn2.protobuff.DataStore.DataResponse;
import pl.nask.hsn2.protobuff.DataStore.DataResponse.ResponseType;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class DataStorePerformanceTest {
	private static final DataStoreConnector DS_CONN = new DataStoreConnectorImpl("http://127.0.0.1:8080/");
	private static final String RBT_HOST_NAME = "localhost";
	private static final String RBT_NOTIFY_EXCH_NAME = "notify";
	private static final String RBT_MAIN_EXCHANGE_NAME = "main";
	private static final String DEFAULT_CONTENT_TYPE = "application/hsn2+protobuf";
	private static final String TEST_TEXT = "This is long text. This is long text. This is long text. ";
	private static final int TEST_TEXT_LENGTH = TEST_TEXT.length();
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStorePerformanceTest.class);

	private static String stringForDataStore;
	private static Path bigFile;
	private static Path smallFile;
	private static MessageSerializer<Operation> defaultSerializer;
	private static ExecutorService executor;
	private static long jobId;

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
		defaultSerializer = new ProtoBufMessageSerializer();
		executor = Executors.newFixedThreadPool(cmdLineOpt.getThreadsNumber());
		jobId = cmdLineOpt.getJobId();
		CountDownLatch latch = new CountDownLatch(2 * cmdLineOpt.getHowManyFiles());
		LOGGER.info(
				"\nStarting with options:\n- files to test = {}\n- threads number = {}\n- job id = {}\n- big file size = {}\n"
						+ "- small file size = {}\n- strings per iteration = {}\n- small files per iteration = {}\n- big files per iteration = {}\n",
				new Object[] { cmdLineOpt.getHowManyFiles(), cmdLineOpt.getThreadsNumber(), cmdLineOpt.getJobId(),
						cmdLineOpt.getBigFileSize(), cmdLineOpt.getSmallFileSize(), cmdLineOpt.getStringFilesLimit(),
						cmdLineOpt.getSmallFilesLimit(), cmdLineOpt.getBigFilesLimit() });

		// Submit all tasks.
		ActionType nextAction = ActionType.STRING;
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

	private static void prepareTempFiles(CmdLineOpt cmdLnOpt) {
		// Prepare test String.
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			sb.append(TEST_TEXT);
		}
		stringForDataStore = sb.toString();

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
		ActionType.STRING.setFilesLimit(cmdLnOpt.getStringFilesLimit());
		ActionType.SMALL_FILE.setFilesLimit(cmdLnOpt.getSmallFilesLimit());
		ActionType.BIG_FILE.setFilesLimit(cmdLnOpt.getBigFilesLimit());
	}

	private static enum ActionType {
		STRING {
			@Override
			public ActionType next() {
				return SMALL_FILE;
			}
		},
		SMALL_FILE {
			@Override
			public ActionType next() {
				return BIG_FILE;
			}
		},
		BIG_FILE {
			@Override
			public ActionType next() {
				return STRING;
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
			ResponseType type = null;
			try {
				switch (actionToDo) {
				case STRING:
					LOGGER.debug("Adding string...");
					dr = DS_CONN.sendPost(stringForDataStore.getBytes(), jobId);
					key = dr.getRef().getKey();
					type = dr.getType();
					break;
				case BIG_FILE:
					LOGGER.debug("Adding big file...");
					try (InputStream bigFileIs = new BufferedInputStream(new FileInputStream(bigFile.toFile()))) {
						dr = DS_CONN.sendPost(bigFileIs, jobId);
						key = dr.getRef().getKey();
						type = dr.getType();
					}
					break;
				case SMALL_FILE:
					LOGGER.debug("Adding small file...");
					try (InputStream smallFileIs = new BufferedInputStream(new FileInputStream(smallFile.toFile()))) {
						dr = DS_CONN.sendPost(smallFileIs, jobId);
						key = dr.getRef().getKey();
						type = dr.getType();
					}
					break;
				}
				LOGGER.debug("Data added. Response(type={}, key={})", type, key);
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
				try (BufferedInputStream bis = new BufferedInputStream(DS_CONN.getResourceAsStream(jobId, referenceId))) {
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
			connFactory.setHost(RBT_HOST_NAME);
			rbtConnection = connFactory.newConnection();
			Channel channel = rbtConnection.createChannel();
			channel.exchangeDeclare(RBT_NOTIFY_EXCH_NAME, "fanout");
			Operation jobFinishedOperation = new JobFinished(jobId, JobStatus.COMPLETED);
			Destination dst = new RbtDestination(RBT_MAIN_EXCHANGE_NAME, "");
			sendOut(jobFinishedOperation, channel, dst, "");
			rbtConnection.close();
		}

		private void sendOut(Operation operation, Channel channel, Destination dest, String replyTo) throws IOException {
			try {
				// Prepare message.
				Message message = defaultSerializer.serialize(operation);
				message.setDestination(dest);
				message.setCorrelationId(null);
				message.setReplyTo(new RbtDestination(RBT_MAIN_EXCHANGE_NAME, replyTo));

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
		private static final String STRING_FILES_LIMIT_DEFAULT = "200";
		private static final String SMALL_FILES_LIMIT_DEFAULT = "10";
		private static final String BIG_FILES_LIMIT_DEFAULT = "1";

		private int howManyFiles;
		private long jobId;
		private int threadsNumber;
		private int smallFileSize;
		private int bigFileSize;
		private int stringFilesLimit;
		private int smallFilesLimit;
		private int bigFilesLimit;

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

			OptionBuilder.withDescription("Strings number per iteration. (Default: " + STRING_FILES_LIMIT_DEFAULT + ")");
			OptionBuilder.withLongOpt("strPerIter");
			OptionBuilder.hasArgs(1);
			OptionBuilder.withArgName("number");
			options.addOption(OptionBuilder.create("spi"));

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
				stringFilesLimit = Integer.valueOf(cmd.getOptionValue("spi", STRING_FILES_LIMIT_DEFAULT));
				smallFilesLimit = Integer.valueOf(cmd.getOptionValue("sfpi", SMALL_FILES_LIMIT_DEFAULT));
				bigFilesLimit = Integer.valueOf(cmd.getOptionValue("bfpi", BIG_FILES_LIMIT_DEFAULT));
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

		public int getStringFilesLimit() {
			return stringFilesLimit;
		}

		public int getSmallFilesLimit() {
			return smallFilesLimit;
		}

		public int getBigFilesLimit() {
			return bigFilesLimit;
		}

	}
}
