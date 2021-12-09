package testing.template;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

interface IStorage {
    FdbTx beginTransaction() throws BackendException;
    FdbStoreDatabase openDatabase(String name) throws BackendException;
    void removeDatabase(FdbStoreDatabase storeDatabase);
    void close() throws BackendException;
    void clear() throws BackendException;
    boolean exists() throws BackendException;
    String getName() throws BackendException;
}

public class FdbStorage implements IStorage {

    protected FDB fdb;
    protected Database database;
    protected DirectorySubspace rootDirectory;


    int VERSION = 620;
    String CLUSTER_FILE_PATH = "";
    String DIRECTORY_OR_GRAPH_NAME = "";

    private final Map<String, FdbStoreDatabase> stores = new ConcurrentHashMap<>();

    public FdbStorage() throws BackendException{
        fdb = FDB.selectAPIVersion(VERSION);
        database = fdb.open(CLUSTER_FILE_PATH);
        try {
            rootDirectory = DirectoryLayer.getDefault().createOrOpen(database, PathUtil.from(DIRECTORY_OR_GRAPH_NAME)).get();
        }
        catch( Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }


    public FdbTx beginTransaction(TransactionContext ctx) throws BackendException {
        ctx.run((tr) -> {
            return null;
        });



        try {
            final Transaction tx = database.createTransaction();
            FdbTx fdbTx = new FdbTx(tx);
            return fdbTx;
        } catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    @Override
    public FdbTx beginTransaction() throws BackendException {
        return null;
    }

    public FdbStoreDatabase openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if(stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            final DirectorySubspace storeDatabase = rootDirectory.createOrOpen(database, PathUtil.from(name)).get();
            FdbStoreDatabase fdbStoreDatabase = new FdbStoreDatabase(name, storeDatabase, this);
            stores.put(name, fdbStoreDatabase);
            return fdbStoreDatabase;
        } catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    public void removeDatabase(FdbStoreDatabase storeDatabase) {
        String storeDatabaseName = storeDatabase.getName();
        if(!stores.containsKey(storeDatabaseName)) {
            throw new IllegalArgumentException();
        }
        stores.remove(storeDatabaseName);
    }

    public void close() throws BackendException {
        if(!stores.isEmpty()) {
            //todo: error handling
        }
        try {
            database.close();
        } catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    public void clear() throws BackendException {
        try {
            rootDirectory.removeIfExists(database).get();
        } catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    public boolean exists() throws BackendException {
        try {
            return DirectoryLayer.getDefault().exists(database, PathUtil.from(DIRECTORY_OR_GRAPH_NAME)).get();
        } catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    public String getName() { return getClass().getSimpleName(); }
}
