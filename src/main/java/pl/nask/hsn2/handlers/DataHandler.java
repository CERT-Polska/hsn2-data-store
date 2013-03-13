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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import kyotocabinet.DB;
import pl.nask.hsn2.DataStore;
import pl.nask.hsn2.KyotoCabinetBytesKeyUtils;
import pl.nask.hsn2.exceptions.EntryNotFoundException;
import pl.nask.hsn2.exceptions.JobNotFoundException;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

/**
 * Main DataStore data handler. It handles POST and GET requests.
 */
@SuppressWarnings("restriction")
public class DataHandler extends AbstractHandler {
	/**
	 * Kyoto Cabinet database.
	 */
	private final DB db;

	/**
	 * Constructor.
	 * 
	 * @param kyotoCabDatabase
	 */
	public DataHandler(DB kyotoCabDatabase) {
		db = kyotoCabDatabase;
	}

	/**
	 * Handles request.
	 */
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
		} catch (IllegalStateException e) {
			handleError(exchange, 500, e);
		} catch (JobNotFoundException e) {
			handleError(exchange, 403, e);
		} catch (EntryNotFoundException e) {
			handleError(exchange, 404, e);
		}
	}

	/**
	 * Handles POST request.
	 * 
	 * @param exchange
	 * @param jobId
	 * @throws IOException
	 */
	private void handlePost(HttpExchange exchange, long jobId) throws IOException {
		LOGGER.info("Post method. {}", exchange.getRequestURI().getPath());

		// Store data and get new ref id for it.
		String dataId = String.valueOf(DataStore.addData(db, exchange.getRequestBody(), jobId));

		// Set response headers.
		Headers headers = exchange.getResponseHeaders();
		headers.set("Content-ID", dataId);
		headers.set("Location", jobId + "/" + dataId);
		String message = "New entry added with id: " + dataId;

		// Send response.
		exchange.sendResponseHeaders(201, message.length());
		exchange.getResponseBody().write(message.getBytes());
		LOGGER.info(message);
	}

	/**
	 * Handles GET request.
	 * 
	 * @param exchange
	 * @param jobId
	 * @param entryId
	 * @throws IOException
	 * @throws EntryNotFoundException
	 */
	private void handleGet(HttpExchange exchange, long jobId, long entryId) throws IOException, EntryNotFoundException {
		LOGGER.info("Get method. {}", exchange.getRequestURI().getPath());

		// Get value from database.
		byte[] key = KyotoCabinetBytesKeyUtils.getDatabaseKey(jobId, entryId);
		byte[] value;
		if ((value = db.get(key)) == null) {
			throw new EntryNotFoundException("Entry not found (jobId=" + jobId + ", entryId=" + entryId + ")");
		}

		// Set headers.
		Headers headers = exchange.getResponseHeaders();
		headers.set("Content-Type", "application/octet-stream");
		exchange.sendResponseHeaders(200, value.length);

		// Send data.
		OutputStream os = exchange.getResponseBody();
		try (BufferedOutputStream bos = new BufferedOutputStream(os)) {
			bos.write(value, 0, value.length);
		}
	}
}
