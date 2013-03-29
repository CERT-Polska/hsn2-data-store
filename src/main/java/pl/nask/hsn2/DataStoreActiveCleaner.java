/*
 * Copyright (c) NASK, NCSC
 * 
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

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pl.nask.hsn2.protobuff.Jobs.JobFinished;
import pl.nask.hsn2.protobuff.Jobs.JobFinishedReminder;
import pl.nask.hsn2.protobuff.Jobs.JobStatus;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Not thread safe. Only one cleaner should be active all the time.
 */
public class DataStoreActiveCleaner implements Runnable {
	public static enum LeaveJobOption {
		NONE, FAILED, ALL
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreActiveCleaner.class);
	private static final boolean AUTO_ACK = true;
	private final String rbtHostName;
	private final String rbtNotifyExchName;
	private final LeaveJobOption leaveJob;
	/**
	 * Set containing jobs id for which cleaning process has been initialized. Job id should be removed when all job
	 * data has been cleared.
	 */
	private final ConcurrentSkipListSet<Long> actualCleaningJobs = new ConcurrentSkipListSet<>();
	private final ExecutorService executor;
	/**
	 * RabbitMQ connection.
	 */
	private Connection rbtConnection;
	private final ConcurrentHashMap<Long, java.sql.Connection> h2Connections;

	/**
	 * Creates new active cleaner.
	 * 
	 * @param rbtServerHostname
	 *            RabbitMQ server host name.
	 * @param rbtNotifyExchangeName
	 *            RabbitMQ notification exchange name.
	 * @param leaveJobValue
	 *            Leave job option, in order to filter out jobs to clean using their completion status.
	 * @param cleaningThreadsNumber
	 *            Number of thread pool of single job cleaner.
	 * @param h2Connector
	 * @param activeJobsSet
	 */
	public DataStoreActiveCleaner(String rbtServerHostname, String rbtNotifyExchangeName, LeaveJobOption leaveJobValue,
			int cleaningThreadsNumber, ConcurrentHashMap<Long, java.sql.Connection> h2ConnectionsPool) {
		h2Connections = h2ConnectionsPool;
		rbtHostName = rbtServerHostname;
		rbtNotifyExchName = rbtNotifyExchangeName;
		leaveJob = leaveJobValue;
		executor = Executors.newFixedThreadPool(cleaningThreadsNumber);
		LOGGER.info("Active cleaner initialized. (leaveJob={}, rbtHost={}, rbtNotifyExch={}, threads={})", new Object[] { leaveJob,
				rbtHostName, rbtNotifyExchName, cleaningThreadsNumber });
	}

	/**
	 * Main execution method.
	 */
	@Override
	public void run() {
		LOGGER.info("Active cleaner started.");
		// Check if cleaner is needed.
		if (leaveJob == LeaveJobOption.ALL) {
			LOGGER.info("Option to leave all job data intact set. Cleaner not needed - exiting.");
			return;
		}

		listenAndClean();

		shutdown();
	}

	/**
	 * Main loop. Listens for JobFinished and JobFinishedReminder and if one appears it starts job removing tasks.
	 */
	private void listenAndClean() {
		try {
			QueueingConsumer consumer = initRabbitMqConnection();
			LOGGER.info("Waiting for messages...");
			while (true) {
				// Listen for message.
				Delivery delivery = consumer.nextDelivery();
				String type = delivery.getProperties().getType();
				LOGGER.debug("Got delivery {}", type);

				// Clean if job finished data.
				try {
					if ("JobFinished".equals(type)) {
						JobFinished jobFinishedData = JobFinished.parseFrom(delivery.getBody());
						startJobDataRemoving(jobFinishedData.getJob(), jobFinishedData.getStatus());
					} else if ("JobFinishedReminder".equals(type)) {
						JobFinishedReminder jobFinishedData = JobFinishedReminder.parseFrom(delivery.getBody());
						startJobDataRemoving(jobFinishedData.getJob(), jobFinishedData.getStatus());
					}
				} catch (InvalidProtocolBufferException e) {
					LOGGER.warn("Invalid message! Expected: " + type, e);
				}
			}
		} catch (ShutdownSignalException e) {
			LOGGER.warn("Shutdown signal received.", e);
		} catch (ConsumerCancelledException e) {
			LOGGER.info("Cancell signal received.", e);
		} catch (InterruptedException e) {
			LOGGER.error("Interrupted.", e);
		} catch (IOException e) {
			LOGGER.error("Connection issue.", e);
		}
	}

	/**
	 * Starts new cleaning task if eligible (according to leaveJob option).
	 * 
	 * @param jobId
	 *            Id of job to clean.
	 * @param jobStatus
	 *            Job status (needed to filter failed jobs).
	 */
	private void startJobDataRemoving(long jobId, JobStatus jobStatus) {
		if (actualCleaningJobs.contains(jobId)) {
			LOGGER.info("Job data clean request ignored. Already cleaning. (jobId={})", jobId);
		} else {
			java.sql.Connection c = h2Connections.remove(jobId);
			if (c == null) {
				LOGGER.info("Job data clean request ignored. Job id not found in active jobs set. (jobId={})", jobId);
			} else {
				try {
					c.close();
					if (isJobStatusEligibleToClean(jobStatus)) {
						LOGGER.info("Job data clean request added. (jobId={})", jobId);
						executor.execute(new DataStoreCleanSingleJob(actualCleaningJobs, jobId));
					} else {
						LOGGER.info("Job data clean request ignored. Job status not eligible. (jobId={}, status={})", jobId,
								jobStatus.toString());
					}
				} catch (SQLException e) {
					LOGGER.warn("Could not close H2 Database connection.", e);
				}
			}
		}
	}

	/**
	 * If {@code leaveJob} is set to NONE - all data will be erased. If {@code leaveJob} is set to FAILED, all data will
	 * be erased but failed jobs will not be erased.
	 * 
	 * @param jobStatus
	 *            Status of job to check.
	 * @return {@code True} if job data should be erased, {@code false} otherwise.
	 */
	private boolean isJobStatusEligibleToClean(JobStatus jobStatus) {
		// As this point we are sure that if leaveJob is not NONE, then it must be FAILED.
		return leaveJob == LeaveJobOption.NONE || jobStatus != JobStatus.FAILED;
	}

	/**
	 * Initialize RabbitMQ connection.
	 * 
	 * @return RabbitMQ consumer.
	 * @throws IOException
	 *             When there's some connection issues.
	 */
	private QueueingConsumer initRabbitMqConnection() throws IOException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost(rbtHostName);
		rbtConnection = connFactory.newConnection();
		Channel channel = rbtConnection.createChannel();
		channel.exchangeDeclare(rbtNotifyExchName, "fanout");
		String queueName = channel.queueDeclare().getQueue();
		channel.queueBind(queueName, rbtNotifyExchName, "");
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, AUTO_ACK, consumer);
		return consumer;
	}

	/**
	 * Cleaner shutdown request. It will not stop ongoing clean tasks but will take no new tasks and then ends when all
	 * actual tasks are completed.
	 */
	public void shutdown() {
		try {
			rbtConnection.close();
		} catch (IOException e) {
			LOGGER.error("Error while closing RabbitMQ connection.", e);
		}
		executor.shutdown();
	}
}
