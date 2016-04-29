/*
 * Copyright (c) NASK, NCSC
 * This file is part of HoneySpider Network 2.1.
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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import pl.nask.hsn2.connector.REST.DataResponse;
import pl.nask.hsn2.connector.REST.DataStoreConnector;
import pl.nask.hsn2.connector.REST.DataStoreConnectorImpl;

public class DataStoreTest {
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
	private static final String DATA_URL = "data";
	private static final String DATA_PATH = DATA_STORE_PATH + DATA_URL;
	private static final String URL_SEPARATOR = "/";
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreTest.class);
	private static final String TEST_STRING = "test";

	private long id = -1;
	private long job = 1234567890;
	private int port = 5560;
	private String urlString = "http://localhost:" + port + URL_SEPARATOR;
	private DataStoreServer server;

	private static DataStoreConnector dsConnector;

	@BeforeClass
	public void beforeClass() throws Exception {
		deleteTestJobData();
		server = new DataStoreServer(port);
		server.start();
		dsConnector = new DataStoreConnectorImpl(urlString);
	}

	@AfterClass
	public void afterClass() throws Exception {
		server.close();
		Thread.sleep(2000);
		deleteTestJobData();
	}

	private void deleteTestJobData() throws IOException {
		// Check if 'data' directory exists.
		File dataDir = new File(DATA_PATH);
		if (Files.exists(dataDir.toPath())) {
			// Dir exists. Delete job files if needed.
			LOGGER.info("\n\nDATABASE FILE = {}\n", DataStore.getDbFileName(job) + ".h2.db");
			Files.deleteIfExists(new File(DataStore.getDbFileName(job) + ".h2.db").toPath());
			Files.deleteIfExists(new File(DataStore.getDbFileName(job) + ".lock.db").toPath());
			Files.deleteIfExists(new File(DataStore.getDbFileName(job) + ".trace.db").toPath());
		} else {
			// Directory does not exists so there is no need to remove job data files. Just create directory.
			Files.createDirectories(dataDir.toPath());
		}
	}

	@Test
	public void addData() throws Exception {
		try (InputStream inputStream = new ByteArrayInputStream(TEST_STRING.getBytes())) {
			DataResponse resp = dsConnector.sendPost(inputStream, job);
			id = resp.getKeyId();
			Assert.assertNotEquals(id, -1);
		}
	}

	@Test(dependsOnMethods = { "addData" })
	public void getData() throws Exception {
		try (BufferedInputStream bis = new BufferedInputStream(dsConnector.getResourceAsStream(job, id))) {
			StringBuilder sb = new StringBuilder();
			int chInt;
			while ((chInt = bis.read()) != -1) {
				sb.append((char) chInt);
			}
			Assert.assertEquals(sb.toString(), TEST_STRING);
		}
	}

	@Test(dependsOnMethods = { "addData" })
	public void getDataRequest() throws IOException {
		String getData = DATA_URL + URL_SEPARATOR + job + URL_SEPARATOR + id;
		URL url = new URL(urlString + getData);
		URLConnection conn = url.openConnection();
		Assert.assertEquals(conn.getHeaderField("Content-type"), "application/octet-stream");
	}

	@Test
	public void addDataRequest() throws IOException {
		String getData = DATA_URL + URL_SEPARATOR + job;
		URL url = new URL(urlString + getData);
		URLConnection conn = url.openConnection();
		conn.setDoOutput(true);

		String data = TEST_STRING;
		try (OutputStream wr = conn.getOutputStream()) {
			wr.write(data.getBytes());
			wr.flush();
			Assert.assertNotEquals(conn.getHeaderField("Content-ID"), null);
		}
	}
}
