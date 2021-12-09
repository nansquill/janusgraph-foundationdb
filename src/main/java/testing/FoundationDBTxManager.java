package testing;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterator;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

public class FoundationDBTxManager {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBTx.class);

    private Database database;
    private final List<Transaction> availableTx = Collections.synchronizedList(new ArrayList<>());

    private final long timeoutInSeconds = 30L;
    private int currentSlotIndex = 0;
    private final int maxTxAlive = 99999;

    public FoundationDBTxManager(Database database) {
        this.database = database;
    }

    public Void commit(List<org.javatuples.KeyValue<byte[], byte[]>> insertQueries, List<byte[]> deleteQueries) throws PermanentBackendException {
        Transaction currentTx = getTx();

        insertQueries.forEach(keyValuePair -> currentTx.set(keyValuePair.getKey(), keyValuePair.getValue()));
        deleteQueries.forEach(value -> currentTx.clear(value));

        Void result = null;
        try {
            if(insertQueries.isEmpty() && deleteQueries.isEmpty()) {
                currentTx.cancel();
            } else {
                result = run(currentTx.commit());
            }
            currentTx.close();
        } catch (IllegalStateException exception) {
            if(currentTx != null) {
                try {
                    currentTx.close();
                } catch (Exception innerException) {
                    log.error("Exception when closing transaction: {}", innerException.getMessage());
                }
            }
            log.error("[RETRY:1] Commit failed with inserts: {}, deletes: {}", insertQueries.size(), deleteQueries.size());
            throw new PermanentBackendException(exception);
        } catch (Exception exception) {
            if(currentTx != null) {
                try {
                    currentTx.close();
                } catch (Exception innerException) {
                    log.error("Exception when closing transaction: {}", innerException.getMessage());
                }
            }
            log.error("Commit failed with inserts: {}, deletes: {}", insertQueries.size(), deleteQueries.size());
            throw new PermanentBackendException(exception);
        }
        return result;
    }

    public byte[] get(byte[] readQueries) {
        Transaction currentTx = getTx();

        CompletableFuture<byte[]> response = currentTx.get(readQueries);

        byte[] result = run(response);

        currentTx.close();

        return result;
    }

    public List<KeyValue> getRange(Range range) {
        Transaction currentTx = getTx();

        CompletableFuture<List<KeyValue>> response = currentTx.getRange(range).asList();

        List<KeyValue> result = run(response);

        currentTx.close();

        return result;
    }

    public List<KeyValue> getRange(KeySelector begin, KeySelector end, int limit) {
        Transaction currentTx = getTx();

        CompletableFuture<List<KeyValue>> response = currentTx.getRange(begin, end, limit).asList();

        List<KeyValue> result = run(response);

        currentTx.close();

        return result;
    }

    public AsyncIterator<KeyValue> getRangeIter(KeySelector begin, KeySelector end, int limit) {

        //todo: currentl out of limitation!
        return createTx().getRange(begin, end, limit).iterator();
    }

    private <T> T run(CompletableFuture<T> transactionResponse) {
        T result = null;
        try {
            result = transactionResponse.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            return result;
        }
    }

    private <T> CompletableFuture<T> runAsync(Function<? super Transaction, ? extends CompletableFuture<T>> transactionWithResponse) {
        Transaction currentTx = database.createTransaction();

        return transactionWithResponse.apply(currentTx);
    }

    // internal
    /**
    public synchronized int getTxSlot() {
        int slot = currentSlotIndex;
        if(availableTx.size() <= slot) {
            availableTx.add(0, createTx());
        }
        else {
            if(availableTx.get(currentSlotIndex) != null) {
                System.out.println("WARNING! Not closed Transaction or transaction working!!! It will be overwritten");
            }
            availableTx.set(currentSlotIndex, createTx());
        }
        currentSlotIndex = ++currentSlotIndex % maxTxAlive;
        return slot;
    }**/

    /**
    public Transaction getTx(int txSlot) {
        Transaction tr = availableTx.get(txSlot);
        if(tr == null) {
            System.out.println("Transaction is empty!");
        }
        return tr;
    }**/

    public Transaction getTx() {
        Transaction tr = createTx();
        availableTx.add(tr);
        return tr;
    }

    private Transaction createTx() {
        return database.createTransaction();
    }

    private Transaction createTx(Executor executor) {
        return database.createTransaction(executor);
    }

    private Transaction createTx(Executor executor, EventKeeper eventKeeper) {
        return database.createTransaction(executor, eventKeeper);
    }

    public synchronized void rollback() {
        availableTx.forEach(tx -> {
            tx.cancel();
            tx.close();
            tx = null;
        });
    }

    /**
    public AsyncIterable<KeyValue> getRange(KVQuery kvQuery) {

        KeySelector startKeySelector = KeySelector.firstGreaterOrEqual(kvQuery.getStart().as((array, offset, limit) -> {
            final byte[] barray = new byte[limit - offset];
            System.arraycopy(array, offset, barray, 0, limit-offset);
            return barray;
        }));

        KeySelector endKeySelector = KeySelector.firstGreaterOrEqual(kvQuery.getEnd().as((array, offset, limit) -> {
            final byte[] barray = new byte[limit - offset];
            System.arraycopy(array, offset, barray, 0, limit-offset);
            return barray;
        }));

        int limit = kvQuery.getLimit();


        return transaction.getRange(startKeySelector, endKeySelector, limit);
    }**/



}
