package testing.template;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FdbTxActions {

    private static final Logger log = LoggerFactory.getLogger(FdbTxActions.class);

    List<byte[]> insertions;
    List<byte[]> deletions;
    Database db;

    List<Transaction> txList = Collections.emptyList();
    final int txCount = 10;

    public FdbTxActions(Database db) {
        this.db = db;
    }

    public void commit(Map<byte[], byte[]> insertions, List<byte[]> deletions) {

        for(int i=0; i<txCount; i++) {
            txList.add(db.createTransaction());
        }
        int splitCount = insertions.size() / txCount;
        int i = 0;
        int txIndex = 0;
        for(Map.Entry<byte[], byte[]> entry : insertions.entrySet()) {
            txList.get(txIndex).set(entry.getKey(), entry.getValue());
            i++;
            if(i % splitCount == 0) {
                txIndex = (txIndex + 1) % txCount;
            }
        }

        splitCount = deletions.size() / txCount;
        txIndex = 0;
        for(int j = 0; j < deletions.size(); j++ ) {
            txList.get(txIndex).clear(deletions.get(j));
            j++;
            if(j % splitCount == 0) {
                txIndex = (txIndex + 1) % txCount;
            }
        }
        runTx(txList);
    }

    private void runTx(List<Transaction> txhList) {
        for(Transaction txh : txList) {
            txh.runAsync(tr -> {
                log.trace("db={}, op=commit, tx={}", "FdbTxActions", txh);
                tr.commit();
                return null;
            }).exceptionally(e -> {
                log.error("db={}, op=commit, tx={} with exception", "FdbTxActions", txh, e);
                return new PermanentBackendException("Could not commit into store", e);
            });
        }
    }
}
