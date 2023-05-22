package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     * @param gengeralAFiledValue
     *              the AFieldValue for NO Grouping
     * @param AFieldSize
     *              use for avg
     *
     */
    private final int gbfield;
    private final  int afield;
    private final Type gbfieldtype;
    private final Op what;
    private HashMap<Field, Integer> aggregatorMap;


    private int generalAFieldValue;

    private HashMap<Field,Integer> aFieldSizeMap;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.afield=afield;
        this.what=what;
        this.gbfieldtype=gbfieldtype;
        this.generalAFieldValue=0;
        this.aggregatorMap= new HashMap<>();
        this.aFieldSizeMap=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gpFiledValue = gbfield == Aggregator.NO_GROUPING ? null : tup.getField(gbfield);
        Field aFiledValue = tup.getField(afield);

        if (what == Op.SUM) {
            if (gpFiledValue == null)
                generalAFieldValue += ((IntField) aFiledValue).getValue();
            else {
                if (aggregatorMap.containsKey(gpFiledValue)) {
                    aggregatorMap.put(gpFiledValue,
                            aggregatorMap.get(gpFiledValue) +
                                    ((IntField) aFiledValue).getValue());
                } else {
                    aggregatorMap.put(gpFiledValue, ((IntField) aFiledValue).getValue());
                }
            }
        } else if (what == Op.MIN) {

            if (gpFiledValue == null) {
                generalAFieldValue = Math.min(generalAFieldValue,
                        ((IntField) aFiledValue).getValue());
            } else {
                if (aggregatorMap.containsKey(gpFiledValue)) {
                    aggregatorMap.put(gpFiledValue,
                            Math.min(aggregatorMap.get(gpFiledValue),
                                    ((IntField) aFiledValue).getValue()));
                } else {
                    aggregatorMap.put(gpFiledValue, ((IntField) aFiledValue).getValue());
                }
            }
        } else if (what == Op.MAX) {
            if (gpFiledValue == null) {
                generalAFieldValue = Math.max(generalAFieldValue,
                        ((IntField) aFiledValue).getValue());
            } else {
                if (aggregatorMap.containsKey(gpFiledValue)) {
                    aggregatorMap.put(gpFiledValue,
                            Math.max(aggregatorMap.get(gpFiledValue),
                                    ((IntField) aFiledValue).getValue()));
                } else {
                    aggregatorMap.put(gpFiledValue, ((IntField) aFiledValue).getValue());
                }
            }
        } else if (what == Op.AVG||what==Op.COUNT) {

            if (gpFiledValue == null) {
                generalAFieldValue += ((IntField) aFiledValue).getValue();
                if (aFieldSizeMap.containsKey(new IntField(0))) {
                    aFieldSizeMap.put(new IntField(0),
                            aFieldSizeMap.get(new IntField(0)) + 1);
                } else {
                    aFieldSizeMap.put(new IntField(0), 1);
                }

            } else {
                if (aggregatorMap.containsKey(gpFiledValue)) {
                    aggregatorMap.put(gpFiledValue,
                            aggregatorMap.get(gpFiledValue) +
                                    ((IntField) aFiledValue).getValue());
                    aFieldSizeMap.put(gpFiledValue,
                            aFieldSizeMap.get(gpFiledValue) + 1);
                } else {
                    aggregatorMap.put(gpFiledValue, ((IntField) aFiledValue).getValue());
                    aFieldSizeMap.put(gpFiledValue, 1);
                }
            }
        }
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
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
        ArrayList<Tuple> tuples= new ArrayList<>();
        TupleDesc td;
        if(gbfield==Aggregator.NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t=new Tuple(td);
            if(what==Op.COUNT) t.setField(0,new IntField(aFieldSizeMap.get(new IntField(0))));
            else if(what==Op.AVG) t.setField(0, new IntField(generalAFieldValue / aFieldSizeMap.get( new IntField(0))));
            else
                t.setField(0,new IntField(generalAFieldValue));

            tuples.add(t);
        }else{
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            for(Map.Entry<Field,Integer> entry: aggregatorMap.entrySet()){
                Tuple t= new Tuple(td);
                t.setField(0, entry.getKey());
                if(what==Op.COUNT) t.setField(1,new IntField(aFieldSizeMap.get(entry.getKey())));
                else if(what==Op.AVG) t.setField(1,new IntField((entry.getValue()/aFieldSizeMap.get(entry.getKey()))));
                else
                    t.setField(1, new IntField(entry.getValue()));
                tuples.add(t);
            }
        }
        return new TupleIterator(td,tuples);
    }

}
