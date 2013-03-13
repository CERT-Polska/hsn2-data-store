package pl.nask.hsn2;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Combines job id and reference id into Kyoto Cabinet database key. Also exctracts job id or reference id from database
 * key.
 */
public final class KyotoCabinetBytesKeyUtils {
	/**
	 * Long type size in bytes provided by JVM.
	 */
	private static final int LONG_BYTES_SIZE = Long.SIZE >> 3;
	/**
	 * Byte buffer used for conversions.
	 */
	private static final ByteBuffer buffer = ByteBuffer.allocate(LONG_BYTES_SIZE * 2);

	/**
	 * This is utility class and can't be instantiated.
	 */
	private KyotoCabinetBytesKeyUtils() {
	}

	/**
	 * Returns key as bytes array.
	 * 
	 * @param jobId
	 *            Job id.
	 * @param refId
	 *            Reference id.
	 * @return Key as bytes array.
	 */
	public static byte[] getDatabaseKey(long jobId, long refId) {
		synchronized (buffer) {
			buffer.putLong(jobId);
			buffer.putLong(refId);
			buffer.flip();
			return buffer.array();
		}
	}

	/**
	 * Returns reference id.
	 * 
	 * @param databaseKey
	 *            Returns Kyoto Cabinet database key.
	 * @return Reference id.
	 */
	public static long getRefereceId(byte[] databaseKey) {
		synchronized (buffer) {
			buffer.clear();
			buffer.put(databaseKey, LONG_BYTES_SIZE, LONG_BYTES_SIZE);
			buffer.flip();
			return buffer.getLong();
		}
	}

	/**
	 * Returns job id.
	 * 
	 * @param databaseKey
	 *            Returns Kyoto Cabinet database key.
	 * @return Job id.
	 */
	public static long getJobId(byte[] databaseKey) {
		synchronized (buffer) {
			buffer.clear();
			buffer.put(databaseKey, 0, LONG_BYTES_SIZE);
			buffer.flip();
			return buffer.getLong();
		}
	}

	/**
	 * This enables the java.library.path to be modified at runtime From a Sun engineer at
	 * http://forums.sun.com/thread.jspa?threadID=707176<br>
	 * <br>
	 * Found on web site: http://nicklothian.com/blog/2008/11/19/modify-javalibrarypath-at-runtime
	 * 
	 * @param dirPath Path to directory that will be added to library path.
	 * @throws IOException When there's no permission to modify {@code java.library.path} property.
	 */
	public static void addDirToJavaLibraryPath(String dirPath) throws IOException {
		try {
			Field field = ClassLoader.class.getDeclaredField("usr_paths");
			field.setAccessible(true);
			String[] paths = (String[]) field.get(null);
			for (int i = 0; i < paths.length; i++) {
				if (dirPath.equals(paths[i])) {
					return;
				}
			}
			String[] tmp = new String[paths.length + 1];
			System.arraycopy(paths, 0, tmp, 0, paths.length);
			tmp[paths.length] = dirPath;
			field.set(null, tmp);
			System.setProperty("java.library.path", System.getProperty("java.library.path") + File.pathSeparator + dirPath);
		} catch (IllegalAccessException e) {
			throw new IOException("Failed to get permissions to set library path");
		} catch (NoSuchFieldException e) {
			throw new IOException("Failed to get field handle to set library path");
		}
	}
}
