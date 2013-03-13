package pl.nask.hsn2.performance;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

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
	private static final int HOW_MANY_FILES = 3000;
	private static final long JOB_ID = 123456789;
	private static final int SMALL_FILE_SIZE = 10000;
	private static final int BIG_FILE_SIZE = 9437000;

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
	private Connection rbtConnection;

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

	private static enum ActionType {
		STRING, SMALL_FILE, BIG_FILE
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

	public static void main(String[] args) throws FileNotFoundException, IOException {
		try (BufferedInputStream smallFile = new BufferedInputStream(new FileInputStream(SMALL_FILE.toFile()))) {
			try (BufferedInputStream bigFile = new BufferedInputStream(new FileInputStream(BIG_FILE.toFile()))) {
				smallFile.mark(Integer.MAX_VALUE);
				bigFile.mark(Integer.MAX_VALUE);
				DataStorePerformanceTest tester = new DataStorePerformanceTest();
				tester.sendDataToDataStore(smallFile, bigFile);
				tester.sendJobFinished();
			}
		}
		Files.delete(SMALL_FILE);
		Files.delete(BIG_FILE);
	}

	private void sendDataToDataStore(InputStream smallFile, InputStream bigFile) {
		long time = System.currentTimeMillis();
		DataResponse dr;
		long key = 0;
		ResponseType type = null;
		ActionType nextAction = ActionType.STRING;
		for (int i = 0; i < HOW_MANY_FILES; i++) {
			try {
				switch (nextAction) {
				case STRING:
					dr = DS_CONN.sendPost(STRING_FOR_DATA_STORE.getBytes(), JOB_ID);
					key = dr.getRef().getKey();
					type = dr.getType();
					nextAction = ActionType.BIG_FILE;
					break;
				case BIG_FILE:
					bigFile.reset();
					dr = DS_CONN.sendPost(bigFile, JOB_ID);
					key = dr.getRef().getKey();
					type = dr.getType();
					nextAction = ActionType.SMALL_FILE;
					break;
				case SMALL_FILE:
					smallFile.reset();
					dr = DS_CONN.sendPost(smallFile, JOB_ID);
					key = dr.getRef().getKey();
					type = dr.getType();
					nextAction = ActionType.STRING;
					break;
				}
				LOGGER.info("Response(type={}, key={})", type, key);
			} catch (IOException e) {
				LOGGER.error("IO error", e);
			}
		}
		time = System.currentTimeMillis() - time;
		LOGGER.info("Files created. Time = {} sec", time / 1000d);
	}

	private void sendJobFinished() {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost(RBT_HOST_NAME);
		try {
			rbtConnection = connFactory.newConnection();
			Channel channel = rbtConnection.createChannel();
			channel.exchangeDeclare(RBT_NOTIFY_EXCH_NAME, "fanout");
			Operation jobFinishedOperation = new JobFinished(JOB_ID, JobStatus.COMPLETED);
			Destination dst = new RbtDestination(RBT_MAIN_EXCHANGE_NAME, "");
			sendOut(jobFinishedOperation, channel, dst, "");
			rbtConnection.close();
		} catch (IOException e) {
			LOGGER.error("JobFinished msg not sent.", e);
		}
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
			BasicProperties.Builder propertiesBuilder = new BasicProperties.Builder().headers(headers).contentType(DEFAULT_CONTENT_TYPE)
					.replyTo(replyToQueueName).type(message.getType());

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
