package correct;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;

import java.io.IOException;
import java.util.function.Consumer;

public class FDBRecordAsyncIterator implements RecordIterator<KeyValueEntry> {
    public FDBRecordAsyncIterator(DirectorySubspace db, FDBStoreTransaction tx, KVQuery kvQuery, AsyncIterator<KeyValue> result, KeySelector keySelector) {
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public KeyValueEntry next() {
        return null;
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
