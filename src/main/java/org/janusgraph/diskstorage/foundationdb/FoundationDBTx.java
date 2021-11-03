package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FoundationDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBTx.class);

    public enum IsolationLevel {SERIALIZABLE, READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE}

    private final IsolationLevel isolationLevel;

    private final FoundationDBTxManager foundationDBTxManager; //singleton

    private final List<org.javatuples.KeyValue<byte[], byte[]>> insertQueries = Collections.synchronizedList(new ArrayList<org.javatuples.KeyValue<byte[], byte[]>>());
    private final List<byte[]> deleteQueries = Collections.synchronizedList(new ArrayList<>());

    public FoundationDBTx(BaseTransactionConfig config, FoundationDBTxManager foundationDBTxManager, IsolationLevel isolationLevel) {
        super(config);
        this.foundationDBTxManager = foundationDBTxManager;
        this.isolationLevel = isolationLevel;
    }

    public void commit() throws BackendException {
        super.commit();
        this.foundationDBTxManager.commit(insertQueries, deleteQueries);
        return;
    }

    @Override
    public synchronized void rollback() {
        try {
            super.rollback();
            foundationDBTxManager.rollback();
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }

    public byte[] get(final byte[] key) throws BackendException{
        return this.foundationDBTxManager.get(key);
    }

    public List<KeyValue> getRange(final FoundationDBRangeQuery query) throws BackendException {
        return this.foundationDBTxManager.getRange(query.getStartKeySelector(), query.getEndKeySelector(), query.getLimit());
    }

    public synchronized Map<KVQuery, List<KeyValue>> getMultiRange(final Collection<FoundationDBRangeQuery> queries) throws BackendException {
        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        //todo fill hash map with results
        return null;
    }

    public void set(org.javatuples.KeyValue<byte[], byte[]> keyValuePair) throws BackendException {
        insertQueries.add(keyValuePair);
    }

    public void clear(byte[] value) throws BackendException {
        deleteQueries.add(value);
    }


    public FoundationDBTx.IsolationLevel getIsolationLevel() { return FoundationDBTx.IsolationLevel.SERIALIZABLE; }

    public void restart() {

    }

    public AsyncIterator<KeyValue> getRangeIter(FoundationDBRangeQuery query, int fetched) {
        final int limit = query.getLimit();
        return foundationDBTxManager.getRangeIter(query.getStartKeySelector(), query.getEndKeySelector(), limit);
    }

    public AsyncIterator<KeyValue> getRangeIter(FoundationDBRangeQuery query) {
        return getRangeIter(query, query.getLimit());
    }



}
