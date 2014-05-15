/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.eagerlogic.entitydb;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author dipacs
 */
public abstract class ATransaction<T> {
    
    private final ReentrantReadWriteLock.WriteLock lock;
    private final DB db;

    public ATransaction(DB db) {
        this.lock = db.getEntityDB().getWriteLock();
        this.db = db;
    }
    
    protected abstract T onExecute(DB db) throws TransactionException;
    
    public T execute() throws TransactionException {
        lock.lock();
        try {
            return onExecute(db);
        } finally {
            lock.unlock();
        }
    }
    
    public T executeSilent() {
        try {
            return execute();
        } catch (TransactionException ex) {
            // TODO swallowing the exception
            return null;
        }
    }
    
}
