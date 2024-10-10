package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFileIterator iterator;
    private TupleDesc prefixTupleDesc;

    // for debug
    public TransactionId getTid() {
        return tid;
    }

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // completed!
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.prefixTupleDesc = computePrefixTupleDesc();
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        // completed!
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // completed!
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // completed!
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.prefixTupleDesc = computePrefixTupleDesc();
        close(); // should reopen
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // completed!
        this.iterator = Database.getCatalog().getDatabaseFile(tableId).iterator(tid);
        this.iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // completed!
        return prefixTupleDesc;
    }

    private TupleDesc computePrefixTupleDesc() {
        TupleDesc origin = Database.getCatalog().getTupleDesc(tableId);
        int n = origin.numFields();
        Type[] types = new Type[n];
        String[] names = new String[n];
        for (int i = 0; i < n; i++) {
            types[i] = origin.getFieldType(i);
            names[i] = tableAlias + "." + origin.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // completed!
        if(iterator == null)
            throw new IllegalStateException("iterator non open");
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // completed!
        if(iterator == null)
            throw new IllegalStateException("iterator non open");
        return iterator.next();
    }

    public void close() {
        // completed!
        iterator.close();
        iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // completed!
        if(iterator == null)
            throw new IllegalStateException("iterator non open");
        close();
        open();
    }
}
