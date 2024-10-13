package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {
    private final ArrayList<Integer> buckets;
    private final int min;
    private final int max;
    private int totalNum;
    private final int width;
    /**
     * Create a new IntHistogram.
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // completed!
        // NOTE: 这是整数桶！桶内是整数的范围
        int actualBucketSize = Math.min(buckets, max - min + 1);
        this.buckets = new ArrayList<>(actualBucketSize);
        this.min = min;
        this.max = max;
        for (int i = 0; i < actualBucketSize; i++) {
            this.buckets.add(0);
        }
        this.width = (int) Math.ceil(1.0 * (max - min + 1) / actualBucketSize); // NOTE:桶的宽度应向上取整
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(v < min || v > max) return; // no-op
        int pos = (v - min) / width;
        buckets.set(pos, buckets.get(pos) + 1);
        totalNum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // 测试用例的要求。
        if(v < min) {
            if(op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ || op == Predicate.Op.NOT_EQUALS)
                return 1.0;
            else return 0.0;
        }
        if(v > max) {
            if(op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.NOT_EQUALS)
                return 1.0;
            else return 0.0;
        }
        double pos = (1.0 * (v - min) / width);
        switch (op) {
            case EQUALS: {
                return 1.0 * buckets.get((int) pos) / width / totalNum;
            }
            case NOT_EQUALS: {
                return 1 - 1.0 * buckets.get((int) pos) / width / totalNum;
            }
            case GREATER_THAN: case GREATER_THAN_OR_EQ: {
                int right = pos == (int) pos ? (int) pos + 1 : (int) Math.ceil(pos); // 右边界
                double part = buckets.get((int) pos) * (right - pos) / width;
                // 只在边界情况算=
                if(pos == right && op == Predicate.Op.GREATER_THAN_OR_EQ) {
                    part = 1.0 * buckets.get((int) pos) / width;
                }
                for (int i = right; i < buckets.size(); i++) {
                    part += buckets.get(i);
                }
                return part / totalNum;
            }
            case LESS_THAN: case LESS_THAN_OR_EQ: {
                int left = (int) pos; // 桶左边界
                double part = buckets.get(left) * (pos - left) / width;
                if(pos == left && op == Predicate.Op.LESS_THAN_OR_EQ) {
                    part = 1.0 * buckets.get(left) / width;
                }
                for (int i = left - 1; i >= 0; i--) {
                    part += buckets.get(i);
                }
                return part / totalNum;
            }
            default:
                throw new IllegalArgumentException("no support op: " + op);
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        throw new RuntimeException("avgSelectivity not implement");
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", totalNum=" + totalNum +
                '}';
    }
}
