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
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pl.nask.hsn2.exceptions.JobNotFoundException;

public class DataStore{
	private static final Logger LOGGER = LoggerFactory.getLogger(DataStore.class);
	private static final String help = "usage: java -jar ...\n-h, --help\tthis help\n-p, --port\tport which dataStore listens (default: 8080)";
	private static final String DATA_STORE_PATH;
	static{
		try {
			String clazzPath = DataStore.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			File clazzFile = new File(clazzPath);
			DATA_STORE_PATH = clazzFile.getParent() + File.separator;
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
	private static final String DATA_PATH = DATA_STORE_PATH + "data";
	private static final String SEQ_PATH = DATA_STORE_PATH + "dataId.seq";
	
	private static long idCount;
	private static int port = 8080;

	private DataStore() {
	}

	public static void main(String[] args) {
	    if (args.length != 0){
	    	String optionName = args[0];
	    	if (optionName.equals("-p") && optionName.equals("--port")){
	    		port = Integer.parseInt(args[1]);
	    		initialize();
				startServer();
		    }
		    else{
		    	if (!optionName.equals("-h") && !optionName.equals("--help")){
		    		System.out.println("Unknown parameter " + optionName);
		    	}
		    	System.out.println(help);
		    }
	    }
	    else{
	    	initialize();
			startServer();
	    }
	}

	private static void startServer() {
		new DataStoreServer(port).start();
	}

	public static long addData(InputStream inputStream, long jobId) throws IOException, JobNotFoundException {
		File dir = getOrCreateJobDirectory(jobId);
		long newId = updateIdCount();

		File file = new File(dir,Long.toString(newId));
		if(!file.exists()) {
			FileOutputStream fileOutputStream = null;
			try {
				fileOutputStream = new FileOutputStream(file);
				IOUtils.copyLarge(inputStream, fileOutputStream);
			} finally {
			    safeClose(fileOutputStream);	
			    IOUtils.closeQuietly(inputStream);
			}
		} else {
			throw new IllegalStateException("Id already exist!");
		}

		return newId;
	}

    private static File getJobDirectory(long job) throws JobNotFoundException {
        File dir = new File(DATA_PATH, Long.toString(job));
    	if(!dir.exists()) {
            throw new JobNotFoundException("Job not found: " + dir);
        }
        return dir;
    }

	synchronized private static	File getOrCreateJobDirectory(long job) throws IllegalStateException{
		File dir = new File(DATA_PATH, Long.toString(job));
    	if(!dir.exists() && !dir.mkdirs()){
        	throw new IllegalStateException("Can not create directory: " + dir);
        }
        return dir;
	}

    public static File getFileForJob(long job, long ref) throws JobNotFoundException {
        File dir = getJobDirectory(job);
        return new File(dir, "" + ref);
    }

    private static void initialize() {
    	idCount = takeIdFromConf();
	}

	private static long takeIdFromConf() {
		BufferedReader bufferedReader = null;
		try{
			bufferedReader = new BufferedReader(new FileReader(SEQ_PATH));
			return Long.parseLong(bufferedReader.readLine());
		}
		catch(Exception e){
			LOGGER.info("Sequence file {} does not exist. New will be created.",SEQ_PATH);
			return 1;
		} finally {
		    safeClose(bufferedReader);
		}
	}

	private static void safeClose(OutputStream os) {
	    if (os != null) {
	        try {
                os.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
	    }
	}

	private static void safeClose(Reader bufferedReader) {
	    if(bufferedReader != null){
            try {
                bufferedReader.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    synchronized private static long updateIdCount() throws IOException{
		long oldId = idCount++;
		FileChannel fileChannel = new RandomAccessFile(SEQ_PATH,"rw").getChannel();
		fileChannel.write(ByteBuffer.wrap((Long.toString(idCount) + "\n").getBytes()));
		fileChannel.close();
		return oldId;
	}
    
    public static String getDataPath() {
    	return DATA_PATH;
    }
}
