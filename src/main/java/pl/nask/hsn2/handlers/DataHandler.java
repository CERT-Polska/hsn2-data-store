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

package pl.nask.hsn2.handlers;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;

import pl.nask.hsn2.DataStore;
import pl.nask.hsn2.exceptions.EntryNotFoundException;
import pl.nask.hsn2.exceptions.JobNotFoundException;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

@SuppressWarnings("restriction")
public class DataHandler extends AbstractHandler {
	private static final String H2_DB_PASSWORD = "hsn2";
	private static final int ARGS_NUMBER_FOR_POST = 2;
	private static final int ARGS_NUMBER_FOR_GET = 3;
	/**
	 * Maps jobId to h2Connector.
	 */
	private final ConcurrentHashMap<Long, Connection> h2Connections;

	public DataHandler(ConcurrentHashMap<Long, Connection> h2ConnectionsPool) {
		h2Connections = h2ConnectionsPool;
	}

	@Override
	protected void handleRequest(HttpExchange exchange, URI uri, String requestMethod) throws IOException {
		String[] args = exchange.getRequestURI().getPath().split("/");
		try {
			if ("GET".equalsIgnoreCase(requestMethod)) {
				if (args.length > ARGS_NUMBER_FOR_GET) {
					handleGet(exchange, Long.parseLong(args[2]), Long.parseLong(args[ARGS_NUMBER_FOR_GET]));
				} else {
					throw new JobNotFoundException("Job or entry id not found.");
				}
			} else if ("POST".equalsIgnoreCase(requestMethod)) {
				if (args.length > ARGS_NUMBER_FOR_POST) {
					handlePost(exchange, Long.parseLong(args[2]));
				} else {
					throw new JobNotFoundException("Job not found.");
				}
			} else {
				throw new UnsupportedOperationException("Unsupported method: " + requestMethod);
			}
		} catch (NumberFormatException e) {
			handleError(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, "Job or entry id is not a number!", e);
		} catch (IllegalStateException | SQLException e) {
			handleError(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, e);
		} catch (JobNotFoundException e) {
			handleError(exchange, HttpStatus.SC_FORBIDDEN, e);
		} catch (EntryNotFoundException e) {
			handleError(exchange, HttpStatus.SC_NOT_FOUND, e);
		}
	}

	private void handlePost(HttpExchange exchange, long jobId) throws IOException, JobNotFoundException, SQLException {
		LOGGER.info("Post method. {}", exchange.getRequestURI().getPath());

		String dataId = String.valueOf(addData(exchange.getRequestBody(), jobId));
		Headers headers = exchange.getResponseHeaders();
		headers.set("Content-ID", dataId);
		headers.set("Location", jobId + "/" + dataId);
		String message = "New entry added with id: " + dataId;

		exchange.sendResponseHeaders(HttpStatus.SC_CREATED, message.length());
		exchange.getResponseBody().write(message.getBytes());
		LOGGER.info(message);
	}

	private long addData(InputStream inputStream, long jobId) throws IOException, SQLException {
		long newId = DataStore.updateIdCount();

		// Check if job's h2 connection exists.
		Connection h2Connection;
		synchronized (h2Connections) {
			h2Connection = h2Connections.get(jobId);
			if (h2Connection == null) {
				h2Connection = createNewDatabase(jobId);
			}
		}

		// Add data to database.
		String sqlQuery = "INSERT INTO JOB_DATA VALUES(?, ?)";
		try (PreparedStatement statement = h2Connection.prepareStatement(sqlQuery)) {
			statement.setLong(1, newId);
			statement.setBlob(2, inputStream);
			int rowsChanged = statement.executeUpdate();
			if (rowsChanged < 1) {
				throw new SQLException("Add data, failure. Nothing inserted.");
			}
		}

		return newId;
	}

	private Connection createNewDatabase(long jobId) throws SQLException {
		// Create new database.
		Connection h2Connection = DriverManager.getConnection("jdbc:h2:" + DataStore.getDbFileName(jobId) + ";LOG=0", "sa", H2_DB_PASSWORD);
		h2Connections.put(jobId, h2Connection);

		// Create new table.
		try (Statement s = h2Connection.createStatement()) {
			s.execute("SET MAX_LOG_SIZE 1");
			s.execute("CREATE TABLE JOB_DATA (ID BIGINT, DATA IMAGE)");
			s.execute("ALTER TABLE JOB_DATA ADD UNIQUE (ID)");
		}

		return h2Connection;
	}

	private void handleGet(HttpExchange exchange, long jobId, long entryId) throws IOException, JobNotFoundException,
			EntryNotFoundException, SQLException {
		LOGGER.info("Get method. {}", exchange.getRequestURI().getPath());
		try (InputStream is = getData(jobId, entryId)) {
			Headers headers = exchange.getResponseHeaders();
			headers.set("Content-Type", "application/octet-stream");

			// Size 0 means: unknown.
			int size = 0;

			exchange.sendResponseHeaders(HttpStatus.SC_OK, size);
			IOUtils.copyLarge(is, exchange.getResponseBody());
		}
	}

	/**
	 * Gets data for given job and entry id. If more that one data is found it will return only first item (such
	 * situation should not happen though).
	 * 
	 * @param jobId
	 *            Job id.
	 * @param entryId
	 *            Entry id.
	 * @return Input stream representing requested data.
	 * @throws SQLException
	 *             When nothing has been found or other SQL issue appears.
	 * @throws JobNotFoundException
	 *             When there's request for data for job that does not have h2 connection created (means no job data
	 *             exists).
	 */
	private InputStream getData(long jobId, long entryId) throws SQLException, JobNotFoundException {
		// Check if job's h2 connection exists.
		Connection h2Connection = h2Connections.get(jobId);
		if (h2Connection == null) {
			throw new JobNotFoundException("Job not found (id=" + jobId + ")");
		}

		Blob data = null;
		try (PreparedStatement statement = h2Connection.prepareStatement("SELECT DATA FROM JOB_DATA WHERE ID=?")) {
			statement.setLong(1, entryId);
			try (ResultSet result = statement.executeQuery()) {
				if (result.next()) {
					data = result.getBlob(1);
				}
			}
		}
		if (data == null) {
			throw new SQLException("No data found.");
		} else {
			return data.getBinaryStream();
		}
	}
}
