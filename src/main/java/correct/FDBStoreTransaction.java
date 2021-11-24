package correct;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import java.awt.*;

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
}
