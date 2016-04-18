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
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

@SuppressWarnings("restriction")
public abstract class AbstractHandler implements HttpHandler {

	protected static final Logger LOGGER = LoggerFactory.getLogger(HttpHandler.class);

	@Override
	public final void handle(HttpExchange exchange) throws IOException {
		Headers headers = exchange.getResponseHeaders();
		headers.set("Content-Type", "text/plain");
		headers.set("Server", "HSN2-DataStore");

		URI uri = exchange.getRequestURI();
		String requestMethod = exchange.getRequestMethod();

		try {
			handleRequest(exchange, uri, requestMethod);
		} catch (Exception e) {
			handleError(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, e);
		} finally {
			OutputStream responseBody = exchange.getResponseBody();
			if (responseBody != null) {
				try {
					responseBody.close();
				} catch (IOException e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		}
	}

	protected final void handleError(HttpExchange exchange, int httpCode, String msg, Exception e) {
		LOGGER.error(msg, e);
		try {
			exchange.sendResponseHeaders(httpCode, msg.length());
			exchange.getResponseBody().write(msg.getBytes());
		} catch (IOException e1) {
			LOGGER.error(e1.getMessage(), e1);
		}
	}

	protected final void handleError(HttpExchange exchange, int httpCode, Exception e) {
		handleError(exchange, httpCode, e.getMessage(), e);
	}

	protected abstract void handleRequest(HttpExchange exchange, URI uri, String requestMethod) throws IOException;
}