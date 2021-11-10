package test;

import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;

import java.util.concurrent.CompletableFuture;

public class FdbStoreDatabase {

    private String name;
    private DirectorySubspace database;
    private final FdbStorage manager;

    public FdbStoreDatabase(String storeName, DirectorySubspace storeDatabase, FdbStorage storeManager) {
        name = storeName;
        database = storeDatabase;
        manager = storeManager;
    }

    public String getName() { return name; }

    public synchronized void close() {
        manager.removeDatabase(this);
    }

    public byte[] get(TransactionContext ctx, StaticBuffer key) throws BackendException {
        byte[] tuple = key.as(FdbUtil.mutateArray);
        byte[] databaseKey = database.pack(tuple);
        return get(ctx, tuple);
    }

    private byte[] get(TransactionContext ctx, byte[] databaseKey) throws BackendException {
        ctx.run((tx) -> {
            return tx.get(databaseKey).join();
        });
        returnn
    }
}
