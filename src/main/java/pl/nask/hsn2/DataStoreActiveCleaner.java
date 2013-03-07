package pl.nask.hsn2;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pl.nask.hsn2.protobuff.Jobs.JobFinished;
import pl.nask.hsn2.protobuff.Jobs.JobFinishedReminder;
import pl.nask.hsn2.protobuff.Jobs.JobStatus;

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
	private final ConcurrentSkipListSet<Long> actualCleaningJobs = new ConcurrentSkipListSet<>();
	private final ExecutorService executor;
	/**
	 * RabbitMQ connection.
	 */
	private Connection rbtConnection;
	private final File dataDir;

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
	 */
	public DataStoreActiveCleaner(String rbtServerHostname, String rbtNotifyExchangeName, LeaveJobOption leaveJobValue,
			int cleaningThreadsNumber, File file) {
		rbtHostName = rbtServerHostname;
		rbtNotifyExchName = rbtNotifyExchangeName;
		leaveJob = leaveJobValue;
		executor = Executors.newFixedThreadPool(cleaningThreadsNumber);
		dataDir = file;
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
				LOGGER.info("Got delivery", delivery.getEnvelope());

				// Clean if job finished data.
				if ("JobFinished".equals(delivery.getProperties().getType())) {
					JobFinished jobFinishedData = JobFinished.parseFrom(delivery.getBody());
					startJobDataRemoving(jobFinishedData.getJob(), jobFinishedData.getStatus());
				} else if ("JobFinishedReminder".equals(delivery.getProperties().getType())) {
					JobFinishedReminder jobFinishedData = JobFinishedReminder.parseFrom(delivery.getBody());
					startJobDataRemoving(jobFinishedData.getJob(), jobFinishedData.getStatus());
				}
			}
		} catch (ShutdownSignalException e) {
			LOGGER.info("Shutdown signal received.", e);
		} catch (ConsumerCancelledException e) {
			LOGGER.info("Cancell signal received.", e);
		} catch (InterruptedException e) {
			LOGGER.info("Interrupted.", e);
		} catch (IOException e) {
			LOGGER.info("Connection issue.", e);
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
			File jobDataDir = new File(dataDir.getAbsolutePath(), "" + jobId);
			if (jobDataDir.exists()) {
				if (isJobStatusEligibleToClean(jobStatus)) {
					LOGGER.info("Job data clean request added. (jobId={})", jobId);
					executor.execute(new DataStoreCleanSingleJob(actualCleaningJobs, jobId, jobDataDir));
				} else {
					LOGGER.info("Job data clean request ignored. Job status not eligible. (jobId={}, status={})", jobId,
							jobStatus.toString());
				}
			} else {
				LOGGER.info("Job data clean request ignored. Data directory not exists. (jobId={})", jobId);
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
		return leaveJob == LeaveJobOption.NONE || (leaveJob == LeaveJobOption.FAILED && jobStatus != JobStatus.FAILED);
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
