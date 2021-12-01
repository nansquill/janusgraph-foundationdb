package org.janusgraph.diskstorage.foundationdb;


import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FoundationDBRecordIterator implements RecordIterator<KeyValueEntry> {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordIterator.class);

    protected DirectorySubspace db;
    protected Iterator<KeyValue> keyValues;
    protected KeyValueEntry current;
    private KeySelector selector;
    private int fetched;

    public FoundationDBRecordIterator(DirectorySubspace db, final Iterator<KeyValue> keyValues, KeySelector selector) {
        this.db = db;
        this.keyValues = keyValues;
        this.selector = selector;
        fetched = 0;
        current = null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        if(current == null) {
            getNextEntry();
        }
        return current != null;
    }

    @Override
    public KeyValueEntry next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        KeyValueEntry next = current;
        current = null;
        return next;
    }

    protected void getNextEntry() {
        while(current == null && keyValues.hasNext()) {
            KeyValue kv = keyValues.next();
            fetched++;
            StaticBuffer key = FoundationDBKeyValueStore.getBuffer(db.unpack(kv.getKey()).getBytes(0));
            if(selector.include(key)) {
                current = new KeyValueEntry(key, FoundationDBKeyValueStore.getBuffer(kv.getValue()));
            }
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
