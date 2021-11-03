/**package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;


public class MockFoundationDBRangeQuery {

    private final KVQuery originalQuery;
    private KeySelector startKeySelector;
    private KeySelector endKeySelector;
    private final int limit;

    public MockFoundationDBRangeQuery(Subspace db, KVQuery kvQuery) {
        originalQuery = kvQuery;
        limit = kvQuery.getLimit();

        final StaticBuffer keyStart = kvQuery.getStart();
        final StaticBuffer keyEnd = kvQuery.getEnd();

        byte[] startKey = (keyStart == null) ?
            db.range().begin : db.pack(keyStart.as(FoundationDBKeyValueStore.ENTRY_FACTORY));
        byte[] endKey = (keyEnd == null) ?
            db.range().end : db.pack(keyEnd.as(FoundationDBKeyValueStore.ENTRY_FACTORY));

        startKeySelector = KeySelector.firstGreaterOrEqual(startKey);
        endKeySelector = KeySelector.firstGreaterOrEqual(endKey);
    }

    public void setStartKeySelector(KeySelector startKeySelector) {
        this.startKeySelector = startKeySelector;
    }

    public void setEndKeySelector(KeySelector endKeySelector) {
        this.endKeySelector = endKeySelector;
    }

    public KVQuery asKVQuery() { return originalQuery; }

    public KeySelector getStartKeySelector() { return startKeySelector; }

    public KeySelector getEndKeySelector() { return endKeySelector; }

    public int getLimit() { return limit; }
}
 **/
