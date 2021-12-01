package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

public class FoundationDBStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBStoreManager.class);

    //Todo ConfigOptions here

    private final Map<String, FoundationDBKeyValueStore> stores;

    protected FDB environment;
    protected Database db;
    protected String rootDirectoryName;
    protected DirectorySubspace rootDirectory;
    protected IsolationLevel isolationLevel;
    protected RangeQueryIteratorMode rangeQueryIteratorMode;

    protected final StoreFeatures features;

    public FoundationDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new HashMap<>();

        //todo check if cache needed
        initialize();
        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .transactional(transactional)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .locking(true)
            .keyOrdered(true)
            .supportsInterruption(false)
            .optimisticLocking(true)
            .multiQuery(true)
            .build();
    }

    private void initialize() throws BackendException {
        //todo check what fdb needs
        environment = FDB.selectAPIVersion(storageConfig.get(FoundationDBConfigOptions.VERSION));
        if (!storageConfig.has(FoundationDBConfigOptions.DIRECTORY) && (storageConfig.has(GRAPH_NAME))) {
            rootDirectoryName = storageConfig.get(GRAPH_NAME);
        }
        rootDirectoryName = storageConfig.get(FoundationDBConfigOptions.DIRECTORY);

        String ph = storageConfig.get(FoundationDBConfigOptions.CLUSTER_FILE_PATH);
        if("default".equals(storageConfig.get(FoundationDBConfigOptions.CLUSTER_FILE_PATH))) {
            db = environment.open(storageConfig.get(FoundationDBConfigOptions.CLUSTER_FILE_PATH));
        } else {
            db = environment.open();
        }

        String isolationLevelText = storageConfig.get(FoundationDBConfigOptions.ISOLATION_LEVEL).toLowerCase().trim();
        switch (isolationLevelText) {
            case "serializable":
                isolationLevel = IsolationLevel.SERIALIZABLE;
                break;
            case "read_committed_no_write":
                isolationLevel = IsolationLevel.READ_COMMITTED_NO_WRITE;
                break;
            case "read_committed_with_write":
                isolationLevel = IsolationLevel.READ_COMMITTED_WITH_WRITE;
                break;
            default:
                throw new PermanentBackendException("Unrecognized isolation level " + isolationLevelText);
        }
        log.info("Isolation level is set to {}", isolationLevel.toString());
        String rangeModeText = storageConfig.get(FoundationDBConfigOptions.GET_RANGE_MODE).toLowerCase().trim();
        switch (rangeModeText) {
            case "iterator":
                rangeQueryIteratorMode = RangeQueryIteratorMode.ASYNC;
                break;
            case "list":
                rangeQueryIteratorMode = RangeQueryIteratorMode.SYNC;
                break;
        }
        log.info("GetRange mode is specified as: {}, record iterator is with: {}", rangeModeText, rangeQueryIteratorMode.toString());

        try {
            // create the root directory to hold the JanusGraph data
            rootDirectory = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(rootDirectoryName)).get();
        } catch (Exception e) {
            throw new PermanentBackendException("Could not create root directory for JanusGraph data {}", e);
        }

    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig baseTransactionConfig) throws BackendException {
        try {
            Transaction tx = null;

            if(transactional) {
                tx = db.createTransaction();
            } else {
                if(baseTransactionConfig instanceof TransactionConfiguration) {
                    if (!((TransactionConfiguration) baseTransactionConfig).isSingleThreaded()) {
                        throw new PermanentBackendException(
                            "FoundationDB does not support non-transactional for multi threaded tx");
                    }
                }
            }
            FoundationDBStoreTransaction ftx = new FoundationDBStoreTransaction(baseTransactionConfig, db, tx, isolationLevel);
            if(log.isTraceEnabled()) {
                log.trace("FoundationDB tx created", new TransactionBegin(ftx.toString()));
            }
            return ftx;
        }
        catch (Exception e) {
            throw new PermanentBackendException("Could not start FoundationDB transaction", e);
        }
    }

    @Override
    public FoundationDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if(stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            final DirectorySubspace storeDb = rootDirectory.createOrOpen(db, PathUtil.from(name)).get();
            log.debug("Opened database {}", name);

            FoundationDBKeyValueStore store = new FoundationDBKeyValueStore(name, storeDb, this);
            stores.put(name, store);
            return null;
        }
        catch (Exception callRightException) {
            throw new PermanentBackendException("Could not open FoundationDB data store", callRightException);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for(Map.Entry<String, KVMutation> mutation : mutations.entrySet()) {
            FoundationDBKeyValueStore store = openDatabase(mutation.getKey());
            KVMutation mutationValue = mutation.getValue();

            if(!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", mutation.getKey());
            } else {
                log.debug("Mutating {}", mutation.getKey());
            }

            if(mutationValue.hasAdditions()) {
                for(KeyValueEntry entry : mutationValue.getAdditions()) {
                    store.insert(entry.getKey(), entry.getValue(), txh, entry.getTtl());
                    log.trace("Insertion on {}: {}", mutation.getKey(), entry);
                }
            }
            if(mutationValue.hasDeletions()) {
                for(StaticBuffer del : mutationValue.getDeletions()) {
                    store.delete(del, txh);
                    log.trace("Deletion on {}: {}", mutation.getKey(), del);
                }
            }
        }
    }

    protected void removeDatabase(FoundationDBKeyValueStore db) {
        if(!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unknown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }

    @Override
    public void close() throws BackendException {
        if(environment != null) {
            if(!stores.isEmpty()) {
                throw new IllegalArgumentException("Cannot shutdown manager since some database are still open");
            }
            try {
                // TODO this looks like a race condition
                //Wait just a little bit before closing so that independent transaction threads can clean up.
                Thread.sleep(30);
            }
            catch (InterruptedException e) {
                //Ignore
            }
            try {
                db.close();
            }
            catch (Exception e) {
                throw new PermanentBackendException("Could not close FoundationDB database", e);
            }
        }
        log.info("");
    }

    private static final Transaction NULL_TRANSACTION = null;

    @Override
    public void clearStorage() throws BackendException {
        if(!stores.isEmpty()) {
            throw new IllegalArgumentException("Cannot delete store, since database is open: " + stores.keySet());
        }
        try {
            rootDirectory.removeIfExists(db).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new PermanentBackendException("Cannot remove store from directory", e);
        }
        log.info("Removed database {} (clearStorage)", db);
        close();
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return DirectoryLayer.getDefault().exists(db, PathUtil.from(rootDirectoryName)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new PermanentBackendException("Cannot check if database exists");
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + rootDirectoryName;
    }

    public enum IsolationLevel {
        READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE, SERIALIZABLE
    }

    public enum RangeQueryIteratorMode { ASYNC, SYNC };

    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }
}
