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
import java.net.URI;

import org.apache.commons.httpclient.HttpStatus;

import com.sun.net.httpserver.HttpExchange;

@SuppressWarnings("restriction")
public class DefaultHandler extends AbstractHandler {

	@Override
	protected void handleRequest(HttpExchange exchange, URI uri, String requestMethod) throws IOException {
		String msg = "";
		int responseCode = -1;

		if ("/".equals(uri.getPath()) && "GET".equalsIgnoreCase(requestMethod)) {
			msg = "DataStore ok!";
			responseCode = HttpStatus.SC_OK;
		} else {
			msg = "Unexpected request!";
			LOGGER.error(msg + " {}", exchange.getRequestURI());
			responseCode = HttpStatus.SC_FORBIDDEN;
		}
		exchange.sendResponseHeaders(responseCode, msg.length());
		exchange.getResponseBody().write(msg.getBytes());
	}
}
