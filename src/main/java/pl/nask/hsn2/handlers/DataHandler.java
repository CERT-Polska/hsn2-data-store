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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;

import pl.nask.hsn2.DataStore;
import pl.nask.hsn2.exceptions.EntryNotFoundException;
import pl.nask.hsn2.exceptions.JobNotFoundException;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

public class DataHandler extends AbstractHandler {
	
	@Override
	protected void handleRequest(HttpExchange exchange, URI uri, String requestMethod) throws IOException{	
		
		String[] args = exchange.getRequestURI().getPath().split("/");
		try {
			if ("GET".equalsIgnoreCase(requestMethod)) {
				if (args.length > 3) {
					handleGet(exchange, Long.parseLong(args[2]), Long.parseLong(args[3]));
				}
				else{
					throw new JobNotFoundException("Job or entry id not found.");
				}
			}
			else if ("POST".equalsIgnoreCase(requestMethod)) {
				if (args.length > 2){
					handlePost(exchange, Long.parseLong(args[2]));
				}
				else{
					throw new JobNotFoundException("Job not found.");
				}
			}
			else{
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

	@SuppressWarnings("restriction")
	private void handlePost(HttpExchange exchange, long jobId) throws IOException, IllegalStateException, JobNotFoundException {
		LOGGER.debug("Post method. {}", exchange.getRequestURI().getPath());

		String dataId = String.valueOf(DataStore.addData(exchange.getRequestBody(), jobId));
		Headers headers = exchange.getResponseHeaders();
		headers.set("Content-ID", dataId);
		headers.set("Location", jobId + "/" + dataId);
		String message = "New entry added with id: " + dataId;
		
		exchange.sendResponseHeaders(201, message.length());
		exchange.getResponseBody().write(message.getBytes());
		LOGGER.debug(message);
	}

	private void handleGet(HttpExchange exchange, long jobId, long entryId) throws IOException, JobNotFoundException, EntryNotFoundException {
		LOGGER.debug("Get method. {}", exchange.getRequestURI().getPath());

		File file = DataStore.getFileForJob(jobId, entryId);
		InputStream inputStream = null;

		try{
			inputStream = new FileInputStream(file);
			Headers headers = exchange.getResponseHeaders();
			headers.set("Content-Type", "application/octet-stream");
			exchange.sendResponseHeaders(200, file.length());

			IOUtils.copyLarge(inputStream, exchange.getResponseBody());
		}
		catch(FileNotFoundException e){
			throw new EntryNotFoundException("Entry not found. Id = " + entryId, e);
		} finally {
		    if (inputStream != null)
		        inputStream.close();
		}
	}
}
