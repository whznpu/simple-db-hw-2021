package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private final  int gbfield;
    private final  int afield;

    private final Op what;
    private final Type gbfieldtype;

    private HashMap<Field,Integer> aggregatorMap;
    private ArrayList<Tuple> tuples;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.afield=afield;
        this.gbfield=gbfield;
        this.what=what;
        this.gbfieldtype=gbfieldtype;
        this.aggregatorMap=new HashMap<>();
        this.tuples= new ArrayList<>();
        if(what!=Op.COUNT){
            throw new IllegalArgumentException("Aggregator is not COUNT");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gpFieldValue= tup.getField(gbfield);
        if(aggregatorMap.containsKey(gpFieldValue)) {
            aggregatorMap.replace(gpFieldValue,
                    aggregatorMap.get(gpFieldValue), aggregatorMap.get(gpFieldValue) + 1);
        }else {
            aggregatorMap.put(gpFieldValue,1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for lab2");
        TupleDesc  td=new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
        for(Map.Entry<Field,Integer> entry: aggregatorMap.entrySet()){
            Tuple t= new Tuple(td);
            t.setField(0, entry.getKey());
            t.setField(1, new IntField(entry.getValue()));
            tuples.add(t);
        }
        return new TupleIterator(td,tuples);
    }

}
