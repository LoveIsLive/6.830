package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.exception.RuntimeReadIOException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private OpIterator child;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // completed!
        this.tid = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // completed!
        return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        // completed!
        child.open();
        called = false;
        super.open();
    }

    public void close() {
        // completed!
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // completed!
        super.rewind();
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // completed!
        if(called) return null;
        called = true;
        List<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            tuples.add(child.next());
        }
        try {
            for (Tuple tuple : tuples) {
                Database.getBufferPool().deleteTuple(tid, tuple);
            }
        } catch (IOException e) {
            throw new RuntimeReadIOException("lab no read IOException");
        }
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(tuples.size()));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // completed!
        return new OpIterator[]{ child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // completed!
        if(children.length != 1)
            throw new IllegalArgumentException("Delete Operator must be one child");
        this.child = children[0];
    }

}
