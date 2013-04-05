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

package pl.nask.hsn2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

import mockit.Delegate;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import pl.nask.hsn2.DataStoreActiveCleaner.LeaveJobOption;
import pl.nask.hsn2.connector.REST.DataStoreConnector;
import pl.nask.hsn2.connector.REST.DataStoreConnectorImpl;
import pl.nask.hsn2.protobuff.DataStore.DataResponse;
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

	private static final String HOST = "localhost";
	private static final int PORT = 7777;
	private static final ConcurrentHashMap<Long, java.sql.Connection> H2_CONN_POOL = new ConcurrentHashMap<>();

	private static DataStoreConnector dsConnector;
	private static DataStoreServer server;

	private CleaningActions nextAction;
	private boolean connCloseRequested;

	@SuppressWarnings({ "rawtypes", "unused" })
	private void mockObjectsWithConnectionException() throws Exception {
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
					void validate() throws IOException {
						connCloseRequested = true;
						throw new IOException("Test IO exception");
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
						Thread.sleep(999999);
						return null;
					}
				};
			}
		};
	}

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
						case TASK_ACCEPTED:
							d = taskAcceptedMsg();
							nextAction = CleaningActions.REMOVE_JOB_1;
							break;
						case REMOVE_JOB_1:
							d = removeJobFinished(1, JobStatus.COMPLETED);
							nextAction = CleaningActions.REMOVE_JOB_2;
							break;
						case REMOVE_JOB_2:
							d = removeJobFinishedReminder(2);
							nextAction = CleaningActions.CANCEL;
							break;
						case REMOVE_JOB_3:
							d = removeJobFinished(3, JobStatus.COMPLETED);
							nextAction = CleaningActions.REMOVE_JOB_3_AGAIN;
							break;
						case REMOVE_JOB_3_AGAIN:
							d = removeJobFinished(3, JobStatus.COMPLETED);
							nextAction = CleaningActions.CANCEL;
							break;
						case REMOVE_JOB_4:
							d = removeJobFinished(4, JobStatus.COMPLETED);
							nextAction = CleaningActions.CANCEL;
							break;
						case REMOVE_JOB_5:
							d = removeJobFinished(5, JobStatus.FAILED);
							nextAction = CleaningActions.CANCEL;
							break;
						case NEVER_RETURN:
							while (true) {
								LOGGER.debug("Never return...");
								Thread.sleep(10000);
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

					private Delivery taskAcceptedMsg() {
						Delivery d;
						Envelope envelope;
						BasicProperties properties;
						JobFinished jf;
						byte[] body;
						envelope = new Envelope(1, false, "", "");
						properties = new BasicProperties.Builder().type("TaskAccepted").build();
						body = new byte[] { 1 };
						d = new Delivery(envelope, properties, body);
						return d;
					}

					private Delivery removeJobFinished(long jobId, JobStatus status) {
						Delivery d;
						Envelope envelope;
						BasicProperties properties;
						JobFinished jf;
						byte[] body;
						envelope = new Envelope(1, false, "", "");
						properties = new BasicProperties.Builder().type("JobFinished").build();
						jf = JobFinished.newBuilder().setJob(jobId).setStatus(status).build();
						body = jf.toByteArray();
						d = new Delivery(envelope, properties, body);
						return d;
					}

					private Delivery removeJobFinishedReminder(long jobId) {
						Delivery d;
						Envelope envelope;
						BasicProperties properties;
						byte[] body;
						envelope = new Envelope(1, false, "", "");
						properties = new BasicProperties.Builder().type("JobFinishedReminder").build();
						JobFinishedReminder jfr = JobFinishedReminder.newBuilder().setJob(jobId).setStatus(JobStatus.COMPLETED).build();
						body = jfr.toByteArray();
						d = new Delivery(envelope, properties, body);
						return d;
					}
				};
			}
		};
	}

	@SuppressWarnings("unused")
	private void mockSingleCleaner() {
		new NonStrictExpectations() {
			@Mocked(methods = { "run" })
			DataStoreCleanSingleJob singleCleaner;
			{
				singleCleaner.run();
				times = 1;
				forEachInvocation = new Object() {
					void validate() {
						LOGGER.info("RUN");
					}
				};
			}
		};
	}

	private static enum CleaningActions {
		REMOVE_JOB_1, REMOVE_JOB_2, REMOVE_JOB_3, CANCEL, NEVER_RETURN, INTERRUPT, SHUTDOWN, IO_EXCEPTION, REMOVE_JOB_3_AGAIN, REMOVE_JOB_4, REMOVE_JOB_5, TASK_ACCEPTED
	}

	@BeforeClass
	public void beforeClass() throws ClassNotFoundException, SQLException {
		server = new DataStoreServer(PORT, H2_CONN_POOL);
		server.start();
		dsConnector = new DataStoreConnectorImpl("http://" + HOST + ":" + PORT + "/");
	}

	@BeforeTest
	public void beforeTest() {
		H2_CONN_POOL.clear();
		LOGGER.info("\n\nBEFORE TEST\n");
	}

	@AfterClass
	public void afterClass() throws SQLException {
		server.close();
	}

	private void addData(long job) throws Exception {
		try (InputStream inputStream = new ByteArrayInputStream("test".getBytes())) {
			DataResponse resp = dsConnector.sendPost(inputStream, job);
			long id = resp.getRef().getKey();
			LOGGER.info("Data added. (job={}, id={})", job, id);
		}
	}

	/**
	 * Standard cleaning tasks. Test creates dirs for job id=1 and id=2 and then it starts cleaner to remove this dirs.
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test
	public void cleanerTest() throws Exception {
		mockObjects();
		addData(1L);
		addData(2L);
		nextAction = CleaningActions.TASK_ACCEPTED;

		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		Thread.sleep(100);
		Assert.assertTrue(connCloseRequested);
		assertH2DbFilesAreDeleted(1);
		assertH2DbFilesAreDeleted(2);
	}

	/**
	 * Check assertion that all H2 DB files for job id has been deleted.
	 * 
	 * @param jobIdToClean
	 *            Job id.
	 */
	private void assertH2DbFilesAreDeleted(long jobIdToClean) {
		Path path;
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".h2.db").toPath();
		Assert.assertTrue(Files.notExists(path), "H2 db file should be removed, but it exists. " + path);
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".lock.db").toPath();
		Assert.assertTrue(Files.notExists(path), "H2 db lock file should be removed, but it exists. " + path);
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".trace.db").toPath();
		Assert.assertTrue(Files.notExists(path), "H2 db trace file should be removed, but it exists. " + path);
	}

	/**
	 * Check assertion that H2 DB file for job id has not been deleted. Lock file should be deleted automatically by H2.
	 * Trace file should never be produced. H2 DB file will be deleted after the test.
	 * 
	 * @param jobIdToClean
	 *            Job id.
	 * @throws IOException
	 */
	private void assertH2DbFilesAreNotDeleted(long jobIdToClean) throws IOException {
		Path path;
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".h2.db").toPath();
		Assert.assertTrue(Files.exists(path), "H2 db file should be removed, but it exists. " + path);
		Files.delete(path);
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".lock.db").toPath();
		Assert.assertTrue(Files.notExists(path), "H2 db lock file should be removed, but it exists. " + path);
		path = new File(DataStore.getDbFileName(jobIdToClean) + ".trace.db").toPath();
		Assert.assertTrue(Files.notExists(path), "H2 db trace file should be removed, but it exists. " + path);
	}

	/**
	 * Standard cleaning tasks. Test creates dirs for job id=1 and id=2 and then it starts cleaner to remove this dirs.
	 * Cleaner is launched with option not to remove failed jobs data, but here all jobs completed successfully.
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test
	public void cleanerTestLeaveFailed() throws Exception {
		mockObjects();
		addData(1L);
		addData(2L);
		nextAction = CleaningActions.TASK_ACCEPTED;

		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.FAILED, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		Thread.sleep(100);
		Assert.assertTrue(connCloseRequested);
		assertH2DbFilesAreDeleted(1);
		assertH2DbFilesAreDeleted(2);
	}

	/**
	 * Cleaned started with option to leave failed jobs only. It does not clean job id=5 because they are marked as
	 * failed.
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test
	public void jobNotEligibleForClean() throws Exception {
		mockObjects();
		addData(5);
		nextAction = CleaningActions.REMOVE_JOB_5;

		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.FAILED, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		Thread.sleep(100);
		Assert.assertTrue(connCloseRequested);
		assertH2DbFilesAreNotDeleted(5);
	}

	/**
	 * Test requests task of cleaning job id=3, and then requests again another task to clean the same job. Test
	 * succeeds when there is no exception. (Except of ConsumerCancelledException which is thrown by test itself).
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test
	public void doubleCleaningTheSameJob() throws Exception {
		mockObjects();
		mockSingleCleaner();
		long job = 3;
		addData(job);
		nextAction = CleaningActions.REMOVE_JOB_3;

		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 2, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		Assert.assertTrue(connCloseRequested);
		Files.deleteIfExists(new File(DataStore.getDbFileName(job) + ".h2.db").toPath());
	}

	/**
	 * Test requests cleaning data for job id=4, but there is no H2 DB files for this job.
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test
	public void jobDieNotExists() throws Exception {
		mockObjects();
		nextAction = CleaningActions.REMOVE_JOB_4;

		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();
	}

	/**
	 * Active cleaner is started with option not to remove job data at all. It has to exit immediately because it is not
	 * needed.
	 * 
	 * Because of LeaveJobOption.ALL service should start and end immediately. If it won't leave as expected, it will
	 * loop forever and therefore test will fail with timeout exceeded.
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test(timeOut = 1000)
	public void cleanerNotNeeded() throws Exception {
		nextAction = CleaningActions.NEVER_RETURN;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.ALL, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();

		Assert.assertFalse(connCloseRequested);
	}

	/**
	 * Test does not starts any cleaning jobs but requests cleaner shutdown.
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test
	public void shutdownCleaner() throws Exception {
		nextAction = CleaningActions.NEVER_RETURN;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		Thread.sleep(100);
		cleanerTask.shutdown();
		Assert.assertTrue(connCloseRequested);
	}

	/**
	 * Test does not starts any cleaning jobs but requests cleaner shutdown. Shutdown throws exception, but it shouldn't
	 * affect whole process.
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test
	public void shutdownCleanerWithException() throws Exception {
		nextAction = CleaningActions.NEVER_RETURN;
		mockObjectsWithConnectionException();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		Thread.sleep(100);
		cleanerTask.shutdown();
		Assert.assertTrue(connCloseRequested);
	}

	/**
	 * Because of invoking InterruptedException in nextDelivery method, cleaning should stop immediately. If it won't
	 * that means test failed (failure on timeout).
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test(timeOut = 1000)
	public void interruptCleaning() throws Exception {
		nextAction = CleaningActions.INTERRUPT;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();
	}

	/**
	 * Because of invoking ShutdownSignalException in nextDelivery method, cleaning should stop immediately. If it won't
	 * that means test failed (failure on timeout).
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test(timeOut = 100000)
	public void shutdownSignal() {
		nextAction = CleaningActions.SHUTDOWN;
		try {
			mockObjects();
		} catch (Exception e) {
			LOGGER.info("Test failed:", e);
			Assert.fail();
		}
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		try {
			cleaner.join();
		} catch (InterruptedException e) {
			LOGGER.info("Test failed:", e);
			Assert.fail();
		}
		Assert.assertTrue(connCloseRequested);
	}

	/**
	 * Because of invoking IOException in nextDelivery method, cleaning should stop immediately. If it won't that means
	 * test failed (failure on timeout).
	 * 
	 * @throws Exception
	 *             When something goes wrong.
	 */
	@Test(timeOut = 1000)
	public void ioExceptionTest() throws Exception {
		nextAction = CleaningActions.IO_EXCEPTION;
		mockObjects();
		DataStoreActiveCleaner cleanerTask = new DataStoreActiveCleaner("", "", LeaveJobOption.NONE, 1, H2_CONN_POOL);
		Thread cleaner = new Thread(cleanerTask);
		cleaner.start();
		cleaner.join();
	}
}
