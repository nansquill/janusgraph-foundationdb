package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

public class FoundationDBRangeQuery {

    private final KVQuery kvQuery;

    private final KeySelector startKeySelector;
    private final KeySelector endKeySelector;
    private final int limit;

    public FoundationDBRangeQuery(Subspace db, KVQuery kvQuery) {
        this.kvQuery = kvQuery;
        limit = kvQuery.getLimit();
        startKeySelector = KeySelector.firstGreaterOrEqual(db.pack(kvQuery.getStart().as(FoundationDBKeyValueStore.ENTRY_FACTORY)));
        endKeySelector = KeySelector.firstGreaterOrEqual(db.pack(kvQuery.getEnd().as(FoundationDBKeyValueStore.ENTRY_FACTORY)));
    }

    public KeySelector getStartKeySelector() { return startKeySelector; }
    public KeySelector getEndKeySelector() { return endKeySelector; }
    public int getLimit() { return kvQuery.getLimit(); }

    public KVQuery getKVQuery() { return kvQuery; }
}
