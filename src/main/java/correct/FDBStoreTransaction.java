package correct;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

import java.awt.*;
import java.util.List;

public class FDBStoreTransaction extends AbstractStoreTransaction {

    private Transaction transaction;

    public FDBStoreTransaction(BaseTransactionConfig config, Database database, Transaction transaction,
                               FDBStoreManager.IsolationLevel isolationLevel) {
        super(config);

    }

    public Transaction getTransaction() {
        return transaction;
    }

    public Cursor openCursor() {
        return new Cursor(0);
    }

    public void closeCursor(Cursor cursor) {}

    public List<KeyValue> getRange(DirectorySubspace db, KVQuery kvQuery) {
    }

    public AsyncIterator<KeyValue> getRangeIterator(DirectorySubspace db, KVQuery kvQuery) {
        return null;
    }
}
