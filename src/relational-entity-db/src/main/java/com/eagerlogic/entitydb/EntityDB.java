package com.eagerlogic.entitydb;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * This class represents a database connection. You need one EntityDB instance
 * per database.
 *
 * @author dipacs
 */
public final class EntityDB {

    public static synchronized EntityDB connect(String url) throws DatabaseException {
        return new EntityDB(url);
    }
    
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private boolean closed = false;
    private boolean newDatabase = false;
    private final RelationalDB rdb;

    private EntityDB(String url) throws DatabaseException {
        newDatabase = !RelationalDB.isDatabaseExists(url);
        rdb = RelationalDB.openOrCreateDatabase(url);
    }

    public synchronized DB getDB() {
        if (closed) {
            throw new IllegalStateException("This db is closed.");
        }

        return new DB(this);
    }

    void put(Entity entity) {
        if (closed) {
            throw new IllegalStateException("This db is closed.");
        }

        WriteLock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            rdb.put(entity);
        } finally {
            lock.unlock();
        }
    }

    void delete(long id) {
        if (closed) {
            throw new IllegalStateException("This db is closed.");
        }

        WriteLock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            rdb.delete(id);
        } finally {
            lock.unlock();
        }
    }

    Entity get(long id) {
        if (closed) {
            throw new IllegalStateException("This db is closed.");
        }

        ReadLock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return rdb.get(id);
        } finally {
            lock.unlock();
        }
    }

    Set<Long> queryKeys(Filter filter) {
        if (closed) {
            throw new IllegalStateException("This db is closed.");
        }

        if (filter == null) {
            throw new NullPointerException("The filter parameter can not be null.");
        }

        ReadLock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return rdb.queryKeys(filter);
        } finally {
            lock.unlock();
        }
    }

    List<Entity> query(Filter filter) {
        if (closed) {
            throw new IllegalStateException("This db is closed.");
        }

        if (filter == null) {
            throw new NullPointerException("The filter parameter can not be null.");
        }

        ReadLock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return rdb.query(filter);
        } finally {
            lock.unlock();
        }
    }

    Entity querySingleton(Filter filter) {
        if (closed) {
            throw new IllegalStateException("This db is closed.");
        }

        if (filter == null) {
            throw new NullPointerException("The filter parameter can not be null.");
        }

        ReadLock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return rdb.querySingleton(filter);
        } finally {
            lock.unlock();
        }
    }

    Entity queryFirst(Filter filter) {
        if (closed) {
            throw new IllegalStateException("This db is closed.");
        }

        if (filter == null) {
            throw new NullPointerException("The filter parameter can not be null.");
        }

        ReadLock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return rdb.queryFirst(filter);
        } finally {
            lock.unlock();
        }
    }

    long querySingletonKey(Filter filter) {
        Set<Long> results = queryKeys(filter);
        if (results.size() > 1) {
            throw new RuntimeException("More than one results are returned by the query.");
        }

        if (results.size() < 1) {
            throw new RuntimeException("No results are returned by the query.");
        }

        for (Long l : results) {
            return l;
        }
        throw new RuntimeException("No results are returned by the query.");
    }

    Long queryFirstKey(Filter filter) {
        Set<Long> results = queryKeys(filter);
        for (Long l : results) {
            return l;
        }
        
        return null;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Closes the database. 
     */
    public synchronized void close() {
        closed = true;
        rdb.close();
    }

    public boolean isNewDatabase() {
        return newDatabase;
    }
    
    ReadLock getReadLock() {
        return readWriteLock.readLock();
    }
    
    WriteLock getWriteLock() {
        return readWriteLock.writeLock();
    }
}
