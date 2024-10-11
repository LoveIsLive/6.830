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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private boolean called;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // completed!
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{ Type.INT_TYPE });
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // completed! Note: for-each should not insert/delete operate
        if(called) return null;
        called = true;
        List<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            tuples.add(child.next());
        }
        try {
            for (Tuple tuple : tuples) {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
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
            throw new IllegalArgumentException("Insert Operator must be one child");
        this.child = children[0];
    }
}
