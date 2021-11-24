package correct;


import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.foundationdb.FoundationDBKeyValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class FDBRecordIterator implements RecordIterator<KeyValueEntry> {

    private DirectorySubspace db;
    private Iterator<KeyValue> keyValues;
    private KeyValueEntry current;
    private KeySelector selector;
    private int fetched;

    public FDBRecordIterator(DirectorySubspace db, final Iterator<KeyValue> keyValues, KeySelector selector) {
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
            current = getNextEntry();
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

    private KeyValueEntry getNextEntry() {
        while(current == null && keyValues.hasNext()) {
            KeyValue kv = keyValues.next();
            fetched++;
            StaticBuffer key = FDBKeyValueStore.getBuffer(db.unpack(kv.getKey()).getBytes(0));
            if(selector.include(key)) {
                current = new KeyValueEntry(key, FDBKeyValueStore.getBuffer(kv.getValue()));
            }
        }
    }

    @Override
    public void remove() {
        RecordIterator.super.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super KeyValueEntry> action) {
        RecordIterator.super.forEachRemaining(action);
    }
}
