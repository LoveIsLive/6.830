package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final int aggfieldIdx;
    private final int gbfieldIdx;
    private OpIterator innerOpIt;
    private final Aggregator.Op op;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param aggfieldIdx The column over which we are computing an aggregate.
     * @param gbfieldIdx The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param op    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int aggfieldIdx, int gbfieldIdx, Aggregator.Op op) {
        // completed!
        this.child = child;
        this.aggfieldIdx = aggfieldIdx;
        this.gbfieldIdx = gbfieldIdx;
        this.op = op;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // completed!
        return gbfieldIdx;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // completed!
        return getTupleDesc().getFieldName(gbfieldIdx);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // completed!
        return aggfieldIdx;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // completed!
        return getTupleDesc().getFieldName(aggfieldIdx);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // completed!
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // completed!
        super.open();
        child.open();
        Aggregator aggregator;
        if(child.getTupleDesc().getFieldType(aggfieldIdx) == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gbfieldIdx, gbfieldIdx == Aggregator.NO_GROUPING ? null :
                    child.getTupleDesc().getFieldType(gbfieldIdx), aggfieldIdx, op);
        } else {
            aggregator = new StringAggregator(gbfieldIdx, gbfieldIdx == Aggregator.NO_GROUPING ? null :
                    child.getTupleDesc().getFieldType(gbfieldIdx), aggfieldIdx, op);
        }
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.rewind();
        innerOpIt = aggregator.iterator();
        innerOpIt.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // completed!
        if(innerOpIt.hasNext()) return innerOpIt.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // completed!
        super.rewind();
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // completed!
        Type[] types;
        if(gbfieldIdx == Aggregator.NO_GROUPING) {
            types = new Type[]{ Type.INT_TYPE };
        } else {
            types = new Type[] { child.getTupleDesc().getFieldType(gbfieldIdx) , Type.INT_TYPE };
        }
        return new TupleDesc(types);
    }

    public void close() {
        // completed!
        super.close();
        innerOpIt.close();
        innerOpIt = null;
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
            throw new IllegalArgumentException("Aggregate Operator must be one child");
        this.child = children[0];
    }

}
