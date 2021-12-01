package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class FoundationDBStoreTransaction extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBStoreTransaction.class);

    private Transaction tx;
    private final FoundationDBStoreManager.IsolationLevel isolationLevel;

    public FoundationDBStoreTransaction(BaseTransactionConfig config, Database database, Transaction transaction,
                                        FoundationDBStoreManager.IsolationLevel isolationLevel) {
        super(config);

        tx = transaction;
        this.isolationLevel = isolationLevel;
    }

    public Transaction getTransaction() {
        return tx;
    }

    public Cursor openCursor() {
        return new Cursor(0);
    }

    public void closeCursor(Cursor cursor) {}

    public List<KeyValue> getRange(FoundationDBRangeQuery rangeQuery) throws PermanentBackendException{
        try {
            List<KeyValue> result = tx.getRange(rangeQuery.getStartKeySelector(), rangeQuery.getEndKeySelector(), rangeQuery.getLimit()).asList().get();
            return result != null ? result : Collections.emptyList();
        } catch (ExecutionException e) {
            log.error("getRange encountered ExecutionException: {}", e.getMessage());
            this.restart();
        } catch (Exception e) {
            log.error("getRange encountered other exception: {}", e.getMessage());
            throw new PermanentBackendException(e);
        }
        throw new PermanentBackendException("GetRange unsuccessful");
    }

    public AsyncIterator<KeyValue> getRangeIterator(FoundationDBRangeQuery rangeQuery) {
        final int limit = rangeQuery.getLimit();
        return tx.getRange(rangeQuery.getStartKeySelector(), rangeQuery.getEndKeySelector(), limit, false, StreamingMode.WANT_ALL).iterator();
    }

    public Map<KVQuery, List<KeyValue>> getMultiRange(Collection<FoundationDBRangeQuery> queries) throws PermanentBackendException {
        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        final List<CompletableFuture<List<KeyValue>>> futures = new LinkedList<>();
        for(FoundationDBRangeQuery rangeQuery : queries) {
            KVQuery kvQuery = rangeQuery.getKVQuery();
            CompletableFuture<List<KeyValue>> f = tx.getRange(rangeQuery.getStartKeySelector(), rangeQuery.getEndKeySelector(), rangeQuery.getLimit()).asList().whenComplete((res, th) -> {
                if(th == null) {
                    if (res == null) {
                        res = Collections.emptyList();
                    }
                    resultMap.put(kvQuery, res);
                } else {
                    log.error("Encountered exception with: {}", th.getCause().getMessage());
                    resultMap.put(kvQuery, Collections.emptyList());
                }
            });
            futures.add(f);
        }

        CompletableFuture<Void> futuresDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            futuresDone.join();
        } catch (Exception e) {
            log.error("Multi-range query encountered transient exception in some futures: {}", e.getCause().getMessage());
        }
        log.debug("get range succeeded with, thread id: {}", Thread.currentThread().getId());
        return resultMap;
    }

    public FoundationDBStoreManager.IsolationLevel getIsolationLevel() { return this.isolationLevel; }

    public void restart() {
    }
}
