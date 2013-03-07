package pl.nask.hsn2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListSet;

import mockit.Delegate;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import pl.nask.hsn2.DataStoreActiveCleaner.LeaveJobOption;
import pl.nask.hsn2.protobuff.Jobs.JobFinished;
import pl.nask.hsn2.protobuff.Jobs.JobFinishedReminder;
import pl.nask.hsn2.protobuff.Jobs.JobStatus;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

public class DataStoreActiveCleanerTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreActiveCleanerTest.class);
	@Mocked
	Connection c;
	@Mocked
	Channel ch;
	@Mocked
	DeclareOk dok;
	@Mocked
	QueueingConsumer consumer;

	private Path dirBase;
	CleaningActions nextAction;
	private boolean connCloseRequested = false;

	@SuppressWarnings({ "rawtypes", "unused" })
	private void mockObjects() throws Exception {
		connCloseRequested = false;
		new NonStrictExpectations() {
			@Mocked
			ConnectionFactory cf;
			{
				// Create new connection.
				cf.newConnection();
				result = c;

				// Create channel.
				c.createChannel();
				result = ch;

				// Close connection.
				c.close();
				forEachInvocation = new Object() {
					void validate() {
						connCloseRequested = true;
					}
				};

				// Declare exchange.
				ch.exchangeDeclare(anyString, anyString);

				// Declare queue.
				ch.queueDeclare();
				result = dok;

				// Get queue name.
				dok.getQueue();

				consumer.nextDelivery();
				result = new Delegate() {
					public Delivery nextDelivery() throws Exception {
						Thread.sleep(100);
						Delivery d = null;
						Envelope envelope;
						BasicProperties properties;
						JobFinished jf;
						byte[] body;
						switch (nextAction) {
						case REMOVE_JOB_1:
							d = removeJob1();
							break;
						case REMOVE_JOB_2:
							d = removeJob2();
							break;
						case REMOVE_JOB_3:
							d = removeJob3();
							nextAction = CleaningActions.REMOVE_JOB_3;
							break;
						case REMOVE_JOB_3_AGAIN:
							d = removeJob3();
							nextAction = CleaningActions.REMOVE_JOB_2;
							break;
						case NEVER_RETURN:
							while (true) {
								LOGGER.info("Never return...");
								Thread.sleep(1000);
							}
						case CANCEL:
							throw new ConsumerCancelledException();
						case INTERRUPT:
							throw new InterruptedException("Test interruption");
						case SHUTDOWN:
							throw new ShutdownSignalException(false, false, null, null);
						case IO_EXCEPTION:
							throw new IOException("Test I/O exception");
						}
						return d;
					}

					private Delivery removeJob1() {
						Delivery d;
						Envelope envelope;
						BasicProperties properties;
						JobFinished jf;
						byte[] body;
						envelope = new Envelope(1L, false, "", "");
						properties = new BasicProperties.Builder().type("JobFinished").build();
						jf = JobFinished.newBuilder().setJob(1L).setStatus(JobStatus.COMPLETED).build();
						body = jf.toByteArray();
						d = new Delivery(envelope, properties, body);
						nextAction = CleaningActions.REMOVE_JOB_2;
						return d;
					}

					private Delivery removeJob2() {
						Delivery d;
						Envelope envelope;
						BasicProperties properties;
						byte[] body;
						envelope = new Envelope(1L, false, "", "");
						properties = new BasicProperties.Builder().type("JobFinishedReminder").build();
						JobFinishedReminder jfr = JobFinishedReminder.newBuilder().setJob(2L).setStatus(JobStatus.COMPLETED).build();
						body = jfr.toByteArray();
						d = new Delivery(envelope, properties, body);
						nextAction = CleaningActions.CANCEL;
						return d;
					}

					private Delivery removeJob3() {
						Delivery d;
						Envelope envelope;
						BasicProperties properties;
						JobFinished jf;
						byte[] body;
						envelope = new Envelope(1L, false, "", "");
						properties = new BasicProperties.Builder().type("JobFinishedReminder").build();
						jf = JobFinished.newBuilder().setJob(3L).setStatus(JobStatus.FAILED).build();
						body = jf.toByteArray();
						d = new Delivery(envelope, properties, body);
						return d;
					}
				};
			}
		};
	}
	
	private DataStoreCleanSingleJob mockSingleCleaner() {
		DataStoreCleanSingleJob singleCleaner = null;
		
		new NonStrictExpectations() {
			{
				newInstance("pl.nask.hsn2.DataStoreCleanSingleJob", withInstanceOf(ConcurrentSkipListSet.class), anyLong, withInstanceOf(File.class));
			}
		};
		
		return null;
	}

	private static enum CleaningActions {
		REMOVE_JOB_1, REMOVE_JOB_2, REMOVE_JOB_3, CANCEL, NEVER_RETURN, INTERRUPT, SHUTDOWN, IO_EXCEPTION, REMOVE_JOB_3_AGAIN
	}

	@Test
	public void cleanerTest() throws Exception {
		mockObjects();
		prepareDirs(new long[] { 1, 2 });
		nextAction = CleaningActions.REMOVE_JOB_1;

		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, dirBase.toFile());
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		Thread.sleep(100);
		Assert.assertTrue(Files.notExists(new File(dirBase.toString(), "1").toPath()));
		Assert.assertTrue(Files.notExists(new File(dirBase.toString(), "2").toPath()));
		Assert.assertTrue(connCloseRequested);

		Files.deleteIfExists(dirBase);
	}

	@Test
	public void doubleCleaningTheSameJob() throws Exception {
		mockObjects();
		prepareDirs(new long[] { 3 });
		nextAction = CleaningActions.REMOVE_JOB_3;

		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 2, dirBase.toFile());
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		Thread.sleep(100);
		Assert.assertTrue(Files.notExists(new File(dirBase.toString(), "3").toPath()));
		Assert.assertTrue(connCloseRequested);

		Files.deleteIfExists(dirBase);
	}

	private void prepareDirs(long[] jobDirectories) throws IOException {
		dirBase = Files.createTempDirectory("hsn2-data-store_");
		for (int i = 0; i < jobDirectories.length; i++) {
			prepareJobDir(jobDirectories[i]);
		}
	}

	private void prepareJobDir(long jobId) throws IOException {
		Path dir = Files.createDirectory(new File(dirBase.toString(), "" + jobId).toPath());
		for (int i = 0; i < 5; i++) {
			File tempFile = new File(dir.toFile(), "someFile-" + i);
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
				writer.write("test");
			}
		}
	}

	@Test(timeOut = 1000)
	public void cleanerNotNeeded() throws Exception {
		nextAction = CleaningActions.NEVER_RETURN;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.ALL, 1, new File(DataStore.DATA_PATH));
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		// Because of LeaveJobOption.ALL service Should start and end immediately.
		// If it won't leave as expected, it will loop forever and therefore test will fail with timeout exceeded.

		Assert.assertFalse(connCloseRequested);
	}

	@Test
	public void shutdownCleaner() throws Exception {
		nextAction = CleaningActions.NEVER_RETURN;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, new File(DataStore.DATA_PATH));
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		Thread.sleep(100);
		cleanerTask.shutdown();
		Assert.assertTrue(connCloseRequested);
	}

	@Test(timeOut = 1000)
	public void interruptCleaning() throws Exception {
		nextAction = CleaningActions.INTERRUPT;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, new File(DataStore.DATA_PATH));
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		// Because of invoking InterruptedException in nextDelivery method, cleaning should stop immediately. If it
		// won't that means test failed (failure on timeout).
	}

	@Test(timeOut = 1000)
	public void shutdownSignal() throws Exception {
		nextAction = CleaningActions.SHUTDOWN;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, new File(DataStore.DATA_PATH));
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		// Because of invoking ShutdownSignalException in nextDelivery method, cleaning should stop immediately. If it
		// won't that means test failed (failure on timeout).
	}

	@Test(timeOut = 1000)
	public void ioExceptionTest() throws Exception {
		nextAction = CleaningActions.IO_EXCEPTION;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, new File(DataStore.DATA_PATH));
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		// Because of invoking IOException in nextDelivery method, cleaning should stop immediately. If it
		// won't that means test failed (failure on timeout).
	}
}
