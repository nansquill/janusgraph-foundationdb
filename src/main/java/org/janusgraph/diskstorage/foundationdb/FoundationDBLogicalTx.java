package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;

import java.util.Collections;
import java.util.List;

public class FoundationDBLogicalTx {

    List<TransactionActions> collectionOfActions = Collections.emptyList();
    Database db;
    final int split = 100;

    public FoundationDBLogicalTx(Database db) {
        this.db = db;
    }

    public int registerTransaction(FoundationDBTx tx, List<FoundationDBTx.Insert> insertions, List<byte[]> deletions)
    {
        int index = collectionOfActions.size();
        collectionOfActions.add(index, new TransactionActions(tx, insertions, deletions));
        try {
            runCommit(index);
        } catch (BackendException e) {
            e.printStackTrace();
        }
        return index;
    }

    public class TransactionActions {
        public List<FoundationDBTx.Insert> insertions;
        public List<byte[]> deletions;
        public FoundationDBTx tx;

        public TransactionActions(FoundationDBTx tx, List<FoundationDBTx.Insert> insertions, List<byte[]> deletions) {
            this.insertions = insertions;
            this.deletions = deletions;
            this.tx = tx;
        }
    }

    public void runCommit(int index) throws BackendException {
        TransactionActions ta = collectionOfActions.get(index);
        List<Transaction> txList = Collections.emptyList();
        Transaction tx = db.createTransaction();
        for(int i=0; i<ta.insertions.size(); i++)
        {
            tx.set(ta.insertions.get(i).getKey(), ta.insertions.get(i).getValue());
            if(i != 0 && i % split == 0) {
                txList.add(tx);
                tx = db.createTransaction();
            }
        }
        for(int i=0; i<ta.deletions.size(); i++)
        {
            tx.clear(ta.deletions.get(i));
            if(i != 0 && i % split == 0) {
                txList.add(tx);
                tx = db.createTransaction();
            }
        }
        txList.add(tx);

        for(Transaction tr : txList) {
            try {
                tr.runAsync(x -> tr.commit());
            } catch (Exception ex) {
                try {
                    tr.cancel();
                    tr.close();
                } catch (Exception e1) {
                    throw new PermanentBackendException("Could not close failed split tr", e1);
                }
                throw new PermanentBackendException("Could not split transactions", ex);
            } finally {
                try {
                    tr.close();
                } catch (Exception ex) {
                    throw new PermanentBackendException("Could not close splitted tr", ex);
                }
            }
        }
    }
}
