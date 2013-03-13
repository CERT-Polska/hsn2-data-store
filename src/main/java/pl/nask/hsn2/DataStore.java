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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import kyotocabinet.DB;

import org.apache.commons.cli.ParseException;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonController;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DataStore implements Daemon {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStore.class);
	private static final String DATA_STORE_PATH;
	static {
		try {
			String clazzPath = DataStore.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			File clazzFile = new File(clazzPath);
			DATA_STORE_PATH = clazzFile.getParent() + File.separator;
		} catch (URISyntaxException e) {
			// Should never happen.
			throw new IllegalArgumentException("Can't parse URL", e);
		}
	}
	public static final String DATA_PATH = DATA_STORE_PATH + "data";
	private static final String SEQ_PATH = DATA_STORE_PATH + "dataId.seq";

	private static long idCount;
	private DataStoreServer server;

	public static void main(final String[] args) throws DaemonInitException, IOException {
		// D:/Prog/kcwin32/java/jkyotocabinet.dll

		// WST add lib path as CLI parameter
		KyotoCabinetBytesKeyUtils.addDirToJavaLibraryPath("D:/Prog/kcwin32/java");

		DataStore ds = new DataStore();
		ds.init(new DaemonContext() {
			@Override
			public DaemonController getController() {
				return null;
			}

			@Override
			public String[] getArguments() {
				return args;
			}
		});
		ds.start();
	}

	/**
	 * Adds data to DataStore.
	 * 
	 * @param kyotoCabDb
	 *            Kyoto Cabinet database.
	 * @param inputStream
	 *            Input stream representing data to store.
	 * @param jobId
	 *            Job id.
	 * @return Newly created data's reference id.
	 * @throws IOException
	 *             When there's some problem with input/output.
	 */
	public static long addData(DB kyotoCabDb, InputStream inputStream, long jobId) throws IOException {
		long newId = updateIdCount();
		byte[] key = KyotoCabinetBytesKeyUtils.getDatabaseKey(jobId, newId);
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			IOUtils.copyLarge(inputStream, baos);
			kyotoCabDb.set(key, baos.toByteArray());
		}
		return newId;
	}

	private static void setIdFromConf() {
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(SEQ_PATH))) {
			idCount = Long.parseLong(bufferedReader.readLine());
		} catch (IOException e) {
			LOGGER.info("Sequence file {} does not exist. New will be created.", SEQ_PATH);
			idCount = 1;
		}
	}

	private synchronized static long updateIdCount() throws IOException {
		long newId = idCount++;
		try (RandomAccessFile rr = new RandomAccessFile(SEQ_PATH, "rw")) {
			try (FileChannel fileChannel = rr.getChannel()) {
				fileChannel.write(ByteBuffer.wrap((Long.toString(idCount) + "\n").getBytes()));
			}
		}
		return newId;
	}

	public static String getDataPath() {
		return DATA_PATH;
	}

	@Override
	public void init(DaemonContext context) throws DaemonInitException {
		DataStoreCmdLineOptions opt = null;
		try {
			opt = new DataStoreCmdLineOptions(context.getArguments());
		} catch (ParseException e) {
			LOGGER.error("Invalid command line options.\n{}", e);
			throw new DaemonInitException("Could not initialize daemon.", e);
		}

		// If help is printed we don't want to start server, only terminate app.
		if (opt.getRbtHostname() != null) {
			// Start server.
			setIdFromConf();
			server = new DataStoreServer(opt.getPort());

			// Start job data cleaner. (Not thread safe. Only one cleaner should be active all the time.)
			new Thread(new DataStoreActiveCleaner(opt.getRbtHostname(), opt.getRbtNotifyExch(), opt.getLeaveData(),
					opt.getCleaningThreadsNumber(), new File(DATA_PATH))).start();
		}
	}

	@Override
	public void start() {
		if (server != null) {
			server.start();
		}
	}

	@Override
	public void stop() {
		server.close();
	}

	@Override
	public void destroy() {
		// Nothing to do.
	}
}
