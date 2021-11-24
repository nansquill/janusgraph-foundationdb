package correct;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.async.AsyncIterator;
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
import test.FdbUtil;

import java.awt.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class FDBKeyValueStore implements OrderedKeyValueStore, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FDBKeyValueStore.class);

    public static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = (array, offset, limit) -> {
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

    public byte[] get(TransactionContext ctx, StaticBuffer key) throws BackendException {
        byte[] tuple = key.as(FdbUtil.mutateArray);
        byte[] databaseKey = database.pack(tuple);
        return get(ctx, tuple);
    }

    private byte[] get(TransactionContext ctx, byte[] databaseKey) throws BackendException {
        ctx.run((tx) -> {
            return tx.get(databaseKey).join();
        });
        return null;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        Transaction tx = getTransaction(txh);
        try {
            byte[] databaseKey = db.pack(key.as(ENTRY_FACTORY));
            log.trace("db={}, op=get, tx={}", name, txh);

            byte[] entry = tx.get(databaseKey).get();
            if (entry != null) {
                return getBuffer(entry);
            } else {
                return null;
            }
        }
        catch (Exception e) {
            log.error("db={}, op=get, tx={} with exception", name, txh, e);
            throw new PermanentBackendException("", e);
        }
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
        if(storeManager.rangeQueryIteratorMode == FDBStoreManager.RangeQueryIteratorMode.SYNC) {
            return getSliceSync(kvQuery, txh);
        } else {
            return getSliceAsync(kvQuery, txh);
        }
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> list, StoreTransaction txh) throws BackendException {
        if(storeManager.rangeQueryIteratorMode == FDBStoreManager.RangeQueryIteratorMode.SYNC) {
            return getSlicesSync(kvQuery, txh);
        } else {
            return getSlicesAsync(kvQuery, txh);
        }
    }

    public RecordIterator<KeyValueEntry> getSliceSync(KVQuery kvQuery, StoreTransaction txh) throws BackendException {
        log.trace("beginning db={}, op=getSliceSync, tx={}", name, txh);
        final FDBStoreTransaction tx = ((FDBStoreTransaction) txh);
        try {
            final List<KeyValue> result = tx.getRange(db, kvQuery);
            log.trace("db={}, op=getSliceSync, tx={} result-count={}", name, txh, result.size());
            return new FDBRecordIterator(db, result.iterator(), kvQuery.getKeySelector());
        }
        catch (Exception e) {
            log.error("db={}, op=getSliceSync, tx={} with exception", name, txh, e);
            throw new PermanentBackendException(e);
        }
    }

    public RecordIterator<KeyValueEntry> getSliceAsync(KVQuery kvQuery, StoreTransaction txh) throws BackendException {
        log.trace("beginning db={}, op=getSliceAsync, tx={}", name, txh);
        final FDBStoreTransaction tx = ((FDBStoreTransaction) txh);
        try {
            final AsyncIterator<KeyValue> result = tx.getRangeIterator(db, kvQuery);
            log.trace("db={}, op=getSliceAsync, tx={} result-count={}", name, txh, result.size());
            return new FDBRecordAsyncIterator(db, tx, kvQuery, result, kvQuery.getKeySelector());
        }
        catch (Exception e) {
            log.error("db={}, op=getSliceAsync, tx={} with exception", name, txh, e);
            throw new PermanentBackendException(e);
        }

    }

    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlicesSync (List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        return null;
    }

    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlicesAsync (List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {
        insert(key, value, txh, true, ttl);
    }

    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, boolean allowOverwrite, Integer ttl) throws BackendException {
        Transaction tx = getTransaction(txh);
        try {
            log.trace("db={}, op=insert, tx={}", name, txh);
            tx.set(db.pack(key.as(ENTRY_FACTORY)), value.as(ENTRY_FACTORY));
        }
        catch(Exception e) {
            log.error("db={}, op=insert, tx={} with exception", name, txh, e);
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        log.trace("Deletion");
        Transaction tx = getTransaction(txh);
        try {
            log.trace("db={}, op=delete, tx={}", name, txh);
            tx.clear(db.pack(key.as(ENTRY_FACTORY)));
        }
        catch (Exception e) {
            log.error("db={}, op=delete, tx={} with exception", name, txh, e);
            throw new PermanentBackendException("Could not remove from store", e);
        }
    }


    protected static StaticBuffer getBuffer(byte[] entry) {
        return new StaticArrayBuffer(entry);
    }
}
