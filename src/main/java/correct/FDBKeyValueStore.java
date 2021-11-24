package correct;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class FDBKeyValueStore implements OrderedKeyValueStore, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FDBKeyValueStore.class);

    private static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = (array, offset, limit) -> {
        final byte[] bArray = new byte[limit - offset];
        System.arraycopy(array, offset, bArray, 0, limit - offset);
        return bArray;
    };

    @VisibleForTesting
    public static Function<Integer, Integer> ttlConverter = ttl -> (int) Math.max(1, Duration.of(ttl, ChronoUnit.SECONDS).toHours());

    private final DirectorySubspace db;
    private final String name;
    private final FDBStoreManager storeManager;
    private boolean isOpen;

    public FDBKeyValueStore(String name, DirectorySubspace directorySubspace, FDBStoreManager storeManager) {
        this.db = directorySubspace;
        this.name = name;
        this.storeManager = storeManager;
        isOpen = true;
    }

    public Configuration getConfiguration() throws BackendException {
        try {
            return storeManager.getStorageConfig();
        }
        catch (Exception e) {
            throw new PermanentBackendException("Could not get storage configuration", e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    private static Transaction getTransaction(StoreTransaction txh) {
        Preconditions.checkNotNull(txh);
        return ((FDBStoreTransaction) txh).getTransaction();
    }

    private Cursor openCursor(StoreTransaction txh) throws BackendException {
        Preconditions.checkNotNull(txh);
        return ((FDBStoreTransaction) txh).openCursor();
    }

    private static void closeCursor(StoreTransaction txh, Cursor cursor) {
        Preconditions.checkNotNull(txh);
        ((FDBStoreTransaction) txh).closeCursor(cursor);
    }

    @Override
    public synchronized void close() throws BackendException {
        try {
            if(isOpen) {
                db.removeIfExists(null); //todo close
            }
        }
        catch (Exception e) {
            throw new PermanentBackendException("Could not close FoundationDB transaction", e);
        }
        if(isOpen) {
            storeManager.removeDatabase(this);
        }
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key, txh) != null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if(getTransaction(txh) == null) {
            log.warn("Attempt to acquire lock with transactions disabled");
        } //else we need no locking
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery kvQuery, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> list, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {
        insert(key, value, txh, true, ttl);
    }

    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, boolean allowOverwrite, Integer ttl) throws BackendException {

    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {

        log.trace("Deletion");
        Transaction tx = getTransaction(txh);
        try {
            log.trace("db={}, op=delete, tx={}", name, txh);
            Object status = db.delete();
            if(status != SUCCESS) {}
        }
        catch (Exception e) {
            throw new PermanentBackendException("Could not remove from store: " + status);
        }
    }

    private static StaticBuffer getBuffer(byte[] entry) {
        return new StaticArrayBuffer(entry);
    }
}
