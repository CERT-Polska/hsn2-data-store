package pl.nask.hsn2;

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
	 * Returns Kyoto Cabinet database key as bytes array.
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
}
