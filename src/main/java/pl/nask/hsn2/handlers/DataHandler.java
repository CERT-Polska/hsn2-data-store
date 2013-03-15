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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.io.IOUtils;

import pl.nask.hsn2.DataStore;
import pl.nask.hsn2.exceptions.EntryNotFoundException;
import pl.nask.hsn2.exceptions.JobNotFoundException;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

@SuppressWarnings("restriction")
public class DataHandler extends AbstractHandler {
	private final Connection h2Connection;
	private final ConcurrentSkipListSet<Long> activeJobsSet;

	public DataHandler(Connection h2dbConnection, ConcurrentSkipListSet<Long> activeJobs) {
		h2Connection = h2dbConnection;
		activeJobsSet = activeJobs;
	}

	@Override
	protected void handleRequest(HttpExchange exchange, URI uri, String requestMethod) throws IOException {
		String[] args = exchange.getRequestURI().getPath().split("/");
		try {
			if ("GET".equalsIgnoreCase(requestMethod)) {
				if (args.length > 3) {
					handleGet(exchange, Long.parseLong(args[2]), Long.parseLong(args[3]));
				} else {
					throw new JobNotFoundException("Job or entry id not found.");
				}
			} else if ("POST".equalsIgnoreCase(requestMethod)) {
				if (args.length > 2) {
					handlePost(exchange, Long.parseLong(args[2]));
				} else {
					throw new JobNotFoundException("Job not found.");
				}
			} else {
				throw new UnsupportedOperationException("Unsupported method: " + requestMethod);
			}
		} catch (NumberFormatException e) {
			handleError(exchange, 500, "Job or entry id is not a number!", e);
		} catch (IllegalStateException | SQLException e) {
			handleError(exchange, 500, e);
		} catch (JobNotFoundException e) {
			handleError(exchange, 403, e);
		} catch (EntryNotFoundException e) {
			handleError(exchange, 404, e);
		}
	}

	private void handlePost(HttpExchange exchange, long jobId) throws IOException, IllegalStateException, JobNotFoundException,
			SQLException {
		LOGGER.info("Post method. {}", exchange.getRequestURI().getPath());

		String dataId = String.valueOf(addData(exchange.getRequestBody(), jobId));
		Headers headers = exchange.getResponseHeaders();
		headers.set("Content-ID", dataId);
		headers.set("Location", jobId + "/" + dataId);
		String message = "New entry added with id: " + dataId;

		exchange.sendResponseHeaders(201, message.length());
		exchange.getResponseBody().write(message.getBytes());
		LOGGER.info(message);
	}

	private long addData(InputStream inputStream, long jobId) throws IOException, SQLException {
		long newId = DataStore.updateIdCount();

		// Check if job's table exists.
		if (!activeJobsSet.contains(jobId)) {
			createTableIfNotExists(jobId);
			activeJobsSet.add(jobId);
		}

		// Add data to database.
		String sqlQuery = "INSERT INTO JOB_" + jobId + " VALUES(?, ?)";
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

	private void createTableIfNotExists(long jobId) throws SQLException {
		String tableName = "JOB_" + jobId;
		DatabaseMetaData dbm = h2Connection.getMetaData();
		// Check if table is there.
		try (ResultSet tables = dbm.getTables(null, null, tableName, null)) {
			boolean tableExists = tables.next();
			if (!tableExists) {
				// Table not exists.
				try (Statement s = h2Connection.createStatement()) {
					boolean resultCreateTable = s.execute("CREATE TABLE " + tableName + " (ID BIGINT, DATA IMAGE)");
					if (resultCreateTable) {
						// Should never happen.
						LOGGER.debug("Create table, failed.");
					} else {
						resultCreateTable = s.execute("ALTER TABLE " + tableName + " ADD UNIQUE (ID)");
						if (resultCreateTable) {
							LOGGER.debug("Create table, done. Make ID unique, failed.");
						} else {
							LOGGER.debug("Create table, done. Make ID unique, done.");
						}
					}
				}
			} else {
				LOGGER.debug("Create table, ignored. Table already exists.");
			}
		}
	}

	private void handleGet(HttpExchange exchange, long jobId, long entryId) throws IOException, JobNotFoundException,
			EntryNotFoundException, SQLException {
		LOGGER.info("Get method. {}", exchange.getRequestURI().getPath());

		try (InputStream is = getData(jobId, entryId)) {
			Headers headers = exchange.getResponseHeaders();
			headers.set("Content-Type", "application/octet-stream");
			// WST poprawic size
			int size = 0;
			exchange.sendResponseHeaders(200, size);
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
	 */
	private InputStream getData(long jobId, long entryId) throws SQLException {
		Blob data = null;
		try (PreparedStatement statement = h2Connection.prepareStatement("SELECT DATA FROM JOB_" + jobId + " WHERE ID=?")) {
			statement.setLong(1, entryId);
			ResultSet result = statement.executeQuery();
			if (result.next()) {
				data = result.getBlob(1);
			}
		}
		if (data == null) {
			throw new SQLException("No data found.");
		} else {
			return data.getBinaryStream();
		}
	}
}
