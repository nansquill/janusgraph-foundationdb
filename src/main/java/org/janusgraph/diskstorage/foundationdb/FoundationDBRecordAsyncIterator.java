package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FoundationDBRecordAsyncIterator extends FoundationDBRecordIterator {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordAsyncIterator.class);

    private final FoundationDBStoreTransaction tx;
    private final KVQuery kvQuery;

    private AsyncIterator<KeyValue> entries;

    protected static final int TRANSACTION_TOO_OLD = 1007;

    public FoundationDBRecordAsyncIterator(DirectorySubspace db, FoundationDBStoreTransaction tx, KVQuery kvQuery, AsyncIterator<KeyValue> result, KeySelector keySelector) {
        super(db, result, keySelector);

        this.tx = tx;
        this.kvQuery = kvQuery;
    }

    @Override
    public void close() throws IOException {
        entries.cancel();
    }

    @Override
    protected void getNextEntry() {
        while (true) {
            try {
                super.getNextEntry();
                break;
            } catch (RuntimeException e) {
                log.info("");
                entries.cancel();
                if(tx.getIsolationLevel() != FoundationDBStoreManager.IsolationLevel.SERIALIZABLE) {
                    Throwable t = e.getCause();
                    while (t != null && !(t instanceof FDBException)) {
                        t = t.getCause();
                    }
                    if(t != null && (((FDBException) t).getCode() == TRANSACTION_TOO_OLD)) {
                        tx.restart();
                        entries = tx.getRangeIterator(new FoundationDBRangeQuery(db, kvQuery));
                        this.keyValues = entries;
                        this.current = null;
                    } else {
                        log.error("The throwable is not reastartable {}", t);
                        throw e;
                    }
                }
            } catch (Exception e) {
                log.error("AsyncIterator next() encountered exception {}", e);
                throw e;
            }
        }
    }

    @Override
    public void remove() {
        entries.remove();
    }
}
