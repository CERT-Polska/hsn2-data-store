package pl.nask.hsn2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DataStoreTestUtils {
	public static Path prepareTempDir() throws IOException {
		Path tempDir = Files.createTempDirectory("hsn2-data-store_");
		for (int i = 0; i < 5; i++) {
			String name = tempDir.toString() + File.separator + i;
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(name))) {
				writer.write("test");
			}
		}
		DataStoreSingleJobCleanerTest.LOGGER.info("Temp dir created: {}", tempDir);
		return tempDir;
	}
}
