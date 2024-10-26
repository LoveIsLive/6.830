package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.exception.TypeMismatchException;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfieldIdx;
    private final Type gbfieldType;
    private final int aggfieldIdx;
    private final Op what;
    private final Map<Field, int[]> map; // gbfieldVal -> [curAggVal, curCount]
    // curCount也只对avg有用，且avg op时，是gbfieldVal -> [curSum, curCount]
    // 使用(curAggVal * curCount + newVal) / (curCount + 1)计算会出现误差(double也是)
    /**
     * Aggregate constructor
     * 
     * @param gbfieldIdx
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldType
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param aggfieldIdx
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfieldIdx, Type gbfieldType, int aggfieldIdx, Op what) {
        // completed!
        this.gbfieldIdx = gbfieldIdx;
        this.gbfieldType = gbfieldType;
        this.aggfieldIdx = aggfieldIdx;
        this.what = what;
        this.map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // completed!
        Field gbField = gbfieldIdx == Aggregator.NO_GROUPING ? null : tup.getField(gbfieldIdx);
        Field aggFieldTemp = tup.getField(aggfieldIdx);
        if(!(aggFieldTemp instanceof IntField))
            throw new TypeMismatchException("IntegerAggregator require tuple must be IntField class");
        if((gbField == null ? null : gbField.getType()) != gbfieldType)
            throw new TypeMismatchException("gbfieldType type mismatch");

        IntField aggField = (IntField) aggFieldTemp;
        map.compute(gbField, (k, v) -> {
            switch (what) {
                case MIN: {
                    if(v == null) return new int[]{ aggField.getValue(), 1};
                    return new int[]{ Math.min(aggField.getValue(), v[0]), v[1] + 1};
                }
                case MAX: {
                    if(v == null) return new int[]{ aggField.getValue(), 1};
                    return new int[]{ Math.max(aggField.getValue(), v[0]), v[1] + 1};
                }
                case SUM: {
                    if(v == null) return new int[]{ aggField.getValue(), 1};
                    return new int[]{ aggField.getValue() + v[0], v[1] + 1};
                }
                case COUNT: {
                    if(v == null) return new int[]{ 1, 1};
                    return new int[]{ v[0] + 1, v[1] + 1};
                }
                case AVG: {
                    if(v == null) return new int[]{ aggField.getValue(), 1};
                    return new int[]{ v[0] + aggField.getValue(), v[1] + 1};
                }
                default: {
                    throw new IllegalArgumentException("operator illegal");
                }
            }
        });
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // completed!
        Type[] types;
        if(gbfieldIdx == Aggregator.NO_GROUPING) {
            types = new Type[]{ Type.INT_TYPE };
        } else {
            types = new Type[] { gbfieldType, Type.INT_TYPE };
        }
        TupleDesc tupleDesc = new TupleDesc(types);
        List<Tuple> tupleList = map.entrySet().stream().map(entry -> {
            Field gbField = entry.getKey();
            int aggV = entry.getValue()[0];
            int cnt = entry.getValue()[1];
            if(what == Op.AVG) aggV /= cnt;
            Tuple tuple = new Tuple(tupleDesc);
            if (gbfieldIdx == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(aggV));
            } else {
                tuple.setField(0, gbField);
                tuple.setField(1, new IntField(aggV));
            }
            return tuple;
        }).collect(Collectors.toList());
// Note that this implementation requires space linear in the number of distinct groups.
// For the purposes of this lab, you do not need to worry about
// the situation where the number of groups exceeds available memory.
        return new TupleIterator(tupleDesc, tupleList);
    }
}
