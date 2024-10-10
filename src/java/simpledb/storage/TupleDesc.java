package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private final TDItem[] tdItems;
    private final HashMap<String, Integer> nameToIdx;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // completed!
        return Arrays.stream(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // completed!
        if(typeAr == null || typeAr.length < 1 || (fieldAr != null && typeAr.length != fieldAr.length))
            throw new IllegalArgumentException("typeAr must contain at least one entry.");
        int n = typeAr.length;
        this.tdItems = new TDItem[n];
        this.nameToIdx = new HashMap<>(n);
        for (int i = 0; i < n; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr == null ? null : fieldAr[i]);
            nameToIdx.putIfAbsent(fieldAr == null ? null : fieldAr[i], i);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // completed!
        if(typeAr == null || typeAr.length < 1)
            throw new IllegalArgumentException("typeAr must contain at least one entry.");
        int n = typeAr.length;
        this.tdItems = new TDItem[n];
        this.nameToIdx = new HashMap<>(n);
        nameToIdx.put(null, 0);
        for (int i = 0; i < n; i++) {
            tdItems[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // completed!
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // completed!
        if(i < 0 || i >= tdItems.length)
            throw new NoSuchElementException("idx out of bound");
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // completed!
        if(i < 0 || i >= tdItems.length)
            throw new NoSuchElementException("i out of bound");
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // completed!
        Integer idx = nameToIdx.get(name);
        if(idx == null)
            throw new NoSuchElementException(name + "not present");
        return idx;
    }

    public boolean isExistFieldName(String name) {
        return nameToIdx.containsKey(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // completed!
        int size = 0;
        for (TDItem tdItem : tdItems) {
            size += tdItem.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // completed!
        int m1 = td1.numFields(), m2 = td2.numFields();
        int n = m1 + m2;
        Type[] typeArr = new Type[n];
        String[] fieldArr = new String[n];
        for (int i = 0; i < m1; i++) {
            typeArr[i] = td1.getFieldType(i);
            fieldArr[i] = td1.getFieldName(i);
        }
        for (int i = m1; i < n; i++) {
            typeArr[i] = td2.getFieldType(i - m1);
            fieldArr[i] = td2.getFieldName(i - m1);
        }
        return new TupleDesc(typeArr, fieldArr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // completed!
        if(o == this) return true;
        if(o instanceof TupleDesc) {
            TupleDesc other = (TupleDesc) o;
            if(tdItems.length != other.numFields()) return false;
            for (int i = 0; i < tdItems.length; i++) {
                if(tdItems[i].fieldType != other.getFieldType(i)) return false;
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // completed!
        StringBuilder sb = new StringBuilder();
        for (TDItem tdItem : tdItems) {
            sb.append(tdItem.fieldType)
                    .append('(')
                    .append(tdItem.fieldName)
                    .append("), ");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
