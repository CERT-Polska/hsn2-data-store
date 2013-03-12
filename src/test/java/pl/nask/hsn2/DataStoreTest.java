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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import pl.nask.hsn2.exceptions.JobNotFoundException;

public class DataStoreTest {
	private long id = -1;
	private long job = 0;
	private int port = 5560;
	private String urlString = "http://127.0.0.1:" + port;
	private DataStoreServer server;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreTest.class);

	@BeforeClass
	public void beforeClass() {
		deleteDirectory("data" + File.separator + job);
		deleteDirectory(DataStore.getDataPath() + File.separator + job);
		server = new DataStoreServer(port);
		server.start();
	}

	private void deleteDirectory(String path) {
		File file = new File(path);
		LOGGER.info("Directory to delete: {}", file.getAbsoluteFile());
		if (file.exists()) {
			LOGGER.info("Job directory exists");
			for (File f : file.listFiles()) {
				if (f.delete()) {
					LOGGER.info("File deleted: {}", f.getAbsoluteFile());
				}
			}
			file.delete();
			LOGGER.info("File deleted: {}", file.getAbsoluteFile());
		} else {
			LOGGER.info("Job directory does NOT exists");
		}
	}

	// WST fix test
	/*
	@Test
	public void addData() throws IOException, IllegalStateException, JobNotFoundException {
		InputStream inputStream = null;
		try {
			inputStream = new ByteArrayInputStream("test".getBytes());
			id = DataStore.addData(inputStream, 0);
			Assert.assertNotEquals(id, -1);
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}
	*/

	// WST fix test
	
//	@Test
//	public void getData() throws IOException, JobNotFoundException {
//		BufferedReader bufferedReader = null;
//		try {
//			bufferedReader = new BufferedReader(new FileReader(DataStore.getDatabaseKey(0, id)));
//			Assert.assertEquals(bufferedReader.readLine(), "test");
//		} finally {
//			if (bufferedReader != null) {
//				bufferedReader.close();
//			}
//		}
//	}

	@Test
	public void getDataRequest() throws IOException {
		String getData = "/data/0/0";
		URL url = new URL(urlString + getData);
		URLConnection conn = url.openConnection();
		Assert.assertEquals(conn.getHeaderField("Content-type"), "application/octet-stream");
	}

	@Test
	public void addDataRequest() throws IOException {
		String getData = "/data/0";
		URL url = new URL(urlString + getData);
		URLConnection conn = url.openConnection();
		conn.setDoOutput(true);

		String data = "test";
		OutputStream wr = null;
		try {
			wr = conn.getOutputStream();
			wr.write(data.getBytes());
			wr.flush();
			Assert.assertNotEquals(conn.getHeaderField("Content-ID"), null);
		} finally {
			if (wr != null) {
				wr.close();
			}
		}
	}

	@AfterClass
	public void afterClass() {
		File file = new File("data" + File.separator + job);
		if (file.exists()) {
			new File("data" + File.separator + job + File.separator + id).delete();
			file.delete();
		}
		server.close();
	}

}
