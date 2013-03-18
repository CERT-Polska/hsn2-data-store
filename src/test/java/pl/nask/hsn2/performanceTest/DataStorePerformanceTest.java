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
import pl.nask.hsn2.connector.REST.DataStoreConnector;
import pl.nask.hsn2.connector.REST.DataStoreConnectorImpl;
import pl.nask.hsn2.protobuff.DataStore.DataResponse;
import pl.nask.hsn2.protobuff.DataStore.DataResponse.ResponseType;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class DataStorePerformanceTest {
	private static final int HOW_MANY_FILES = 1000;
	private static final long JOB_ID = 5555;
	private static final int THREADS_NUMBER = 10;
	private static final int SMALL_FILE_SIZE = 10000000;
//	private static final int BIG_FILE_SIZE = 9437000;
	private static final int BIG_FILE_SIZE = 35000000;

	private static final DataStoreConnector DS_CONN = new DataStoreConnectorImpl("http://127.0.0.1:8080/");
	private static final String RBT_HOST_NAME = "localhost";
	private static final String RBT_NOTIFY_EXCH_NAME = "notify";
	private static final String RBT_MAIN_EXCHANGE_NAME = "main";
	private static final String DEFAULT_CONTENT_TYPE = "application/hsn2+protobuf";

	private static final String TEST_TEXT = "This is long text. This is long text. This is long text. ";
	private static final int TEST_TEXT_LENGTH = TEST_TEXT.length();
	private static final String STRING_FOR_DATA_STORE;
	private static final Path BIG_FILE;
	private static final Path SMALL_FILE;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStorePerformanceTest.class);
	private static final MessageSerializer<Operation> DEFAULT_SERIALIZER = new ProtoBufMessageSerializer();
	private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(THREADS_NUMBER);

	static {
		// Prepare test String.
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			sb.append(TEST_TEXT);
		}
		STRING_FOR_DATA_STORE = sb.toString();

		try {
			// Prepare big file.
			BIG_FILE = Files.createTempFile("hsn2-data-store-perfTest-", "");
			createTempFile(BIG_FILE, BIG_FILE_SIZE);

			// Prepare small file.
			SMALL_FILE = Files.createTempFile("hsn2-data-store-perfTest-", "");
			createTempFile(SMALL_FILE, SMALL_FILE_SIZE);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void createTempFile(Path file, int fileSize) throws IOException {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(file.toFile()))) {
			int size = 0;
			while (size < fileSize) {
				bw.write(TEST_TEXT);
				size += TEST_TEXT_LENGTH;
			}
		}
	}

	public static void main(String[] args) throws IOException {
		// Init.
		long time = System.currentTimeMillis();
		CountDownLatch latch = new CountDownLatch(2 * HOW_MANY_FILES);
		ActionType nextAction = ActionType.STRING;

		// Submit all tasks.
		for (int i = 0; i < HOW_MANY_FILES; i++) {
			// Submit add data task. (Which will run receive data task when finished.)
			EXECUTOR.execute(new AddDataTask(nextAction, latch));
			nextAction = nextAction.next();
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
		EXECUTOR.execute(new DeleteJobDataTask());

		// Cleaning.
		Files.delete(SMALL_FILE);
		Files.delete(BIG_FILE);
		EXECUTOR.shutdown();
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

		public abstract ActionType next();
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
					dr = DS_CONN.sendPost(STRING_FOR_DATA_STORE.getBytes(), JOB_ID);
					key = dr.getRef().getKey();
					type = dr.getType();
					break;
				case BIG_FILE:
					LOGGER.debug("Adding big file...");
					try (InputStream bigFile = new BufferedInputStream(new FileInputStream(BIG_FILE.toFile()))) {
						dr = DS_CONN.sendPost(bigFile, JOB_ID);
						key = dr.getRef().getKey();
						type = dr.getType();
					}
					break;
				case SMALL_FILE:
					LOGGER.debug("Adding small file...");
					try (InputStream smallFile = new BufferedInputStream(new FileInputStream(SMALL_FILE.toFile()))) {
						dr = DS_CONN.sendPost(smallFile, JOB_ID);
						key = dr.getRef().getKey();
						type = dr.getType();
					}
					break;
				}
				LOGGER.info("Data added. Response(type={}, key={})", type, key);
				EXECUTOR.execute(new GetDataTask(key, latch));
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
				try (BufferedInputStream bis = new BufferedInputStream(DS_CONN.getResourceAsStream(JOB_ID, referenceId))) {
					StringBuilder sb = new StringBuilder();
					int chInt;
					int size = 0;
					while ((chInt = bis.read()) != -1) {
						sb.append((char) chInt);
						size++;
					}
					LOGGER.info("Data received. Stream size = {}", size);
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
			Operation jobFinishedOperation = new JobFinished(JOB_ID, JobStatus.COMPLETED);
			Destination dst = new RbtDestination(RBT_MAIN_EXCHANGE_NAME, "");
			sendOut(jobFinishedOperation, channel, dst, "");
			rbtConnection.close();
		}

		private void sendOut(Operation operation, Channel channel, Destination dest, String replyTo) throws IOException {
			try {
				// Prepare message.
				Message message = DEFAULT_SERIALIZER.serialize(operation);
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
}
