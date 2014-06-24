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
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pl.nask.hsn2.handlers.DataHandler;
import pl.nask.hsn2.handlers.DefaultHandler;

import com.sun.net.httpserver.HttpServer;

@SuppressWarnings("restriction")
public class DataStoreServer {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreServer.class);
	private HttpServer server;

	public DataStoreServer(int port) throws ClassNotFoundException {
		InetSocketAddress addr = new InetSocketAddress(port);
		try {
			server = HttpServer.create(addr, 0);
		} catch (IOException e) {
			throw new RuntimeException("Server error.", e);
		}
		server.createContext("/", new DefaultHandler());
		server.createContext("/data", new DataHandler());
		server.setExecutor(Executors.newCachedThreadPool());
		LOGGER.info("Server is listening on port {}", port);
	}

	public void start() {
		server.start();
	}

	public void close() throws SQLException {
		server.stop(0);
		LOGGER.info("Server is stopped!");
	}
}
