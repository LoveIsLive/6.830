package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private final Predicate predicate;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // completed!
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // completed!
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // completed!
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // completed!
        child.open();
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
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // completed!
        while (child.hasNext()) {
            Tuple tuple = child.next();
            if(predicate.filter(tuple)) return tuple;
        }
        return null;
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
            throw new IllegalArgumentException("Filter Operator must be one child");
        this.child = children[0];
    }

}
