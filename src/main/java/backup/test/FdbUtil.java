package backup.test;

import org.janusgraph.diskstorage.StaticBuffer;

public class FdbUtil {

    public static StaticBuffer.Factory<byte[]> mutateArray = (byte[] array, int offset, int limit) -> {
        final byte[] resultArray = new byte[limit - offset];
        System.arraycopy(array, offset, resultArray, 0, limit - offset);
        return resultArray;
    };
}
