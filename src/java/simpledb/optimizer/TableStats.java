package simpledb.optimizer;

import com.sun.jdi.Value;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.time.temporal.ValueRange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    private class IntPair{
        private int maxValue;
        private int minValue;
        IntPair(int maxValue,int minValue){
            this.maxValue=maxValue;
            this.minValue=minValue;
        }
        private void setValue(int maxValue,int minValue){
            this.minValue=Math.min(this.minValue,minValue);
            this.maxValue=Math.max(this.maxValue,maxValue);
        }
    }
    private class StringPair{
        private String maxValue;
        private String minValue;
        StringPair(String maxValue,String minValue){
            this.maxValue=maxValue;
            this.minValue=minValue;
        }
        private void setValue(String maxValue,String minValue){
            this.minValue= String.valueOf(Math.min(StringHistogram.stringToInt(minValue),StringHistogram.stringToInt(this.minValue)));
            this.maxValue= String.valueOf(Math.max(StringHistogram.stringToInt(maxValue),StringHistogram.stringToInt(this.maxValue)));
        }
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    private final int tableid;
    private int ioCostPerpage;

    private int numTuples;
    private HeapFile f;


    private ArrayList<Tuple> tuples;

    private HashMap<Integer,IntPair> IntValueMap;
    private HashMap<Integer,StringPair> StringValueMap;
    private HashMap<Integer,IntHistogram> IntHistorgramMap;

    private HashMap<Integer,StringHistogram> StringHistogramMap;
    // fieldId IntHistogram
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

        // the tuples may be saved in the class, so it doesn't need to be scanned twice
        this.tableid=tableid;
        this.ioCostPerpage=ioCostPerPage;
        this.numTuples=0;
        this.IntHistorgramMap= new HashMap<>();
        this.StringHistogramMap=new HashMap<>();
        this.StringValueMap=new HashMap<>();
        this.IntValueMap =new HashMap<>();
        this.tuples= new ArrayList<>();


        this.f= (HeapFile) Database.getCatalog().getDatabaseFile(tableid);

        for(int i=0;i<f.getTupleDesc().numFields();i++){
            if(f.getTupleDesc().getFieldType(i)==Type.INT_TYPE){
                IntPair pair= new IntPair(0,0);
                IntValueMap.put(i,pair);
            }else{
                StringHistogram stringH=new StringHistogram(10);
                StringHistogramMap.put(i,stringH);
            }
        }

        long start=System.currentTimeMillis();


        DbFileIterator it=f.iterator(null);
        try {
            it.open();

            while (true){
                if(!it.hasNext()) break;
                numTuples++;
                Tuple t=it.next();
                tuples.add(t);
                for(int i=0;i<t.getTupleDesc().numFields();i++){
                    if(t.getTupleDesc().getFieldType(i)==Type.INT_TYPE) {
                        IntField intV = (IntField) t.getField(i);
                        IntValueMap.get(i).setValue(intV.getValue(), intV.getValue());
                    }else{
                        StringField stringV=(StringField) t.getField(i);
                        StringHistogramMap.get(i).addValue(stringV.getValue());
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException(e);
        }

        long end=System.currentTimeMillis();
        System.out.println("Scan time: "+(end-start));

        assert (tuples.size()==numTuples);
        // calculate the table statical information
        for(Map.Entry<Integer,IntPair> entry :IntValueMap.entrySet()){
            IntHistogram intH= new IntHistogram(10,
                    entry.getValue().minValue,entry.getValue().maxValue);
            IntHistorgramMap.put(entry.getKey(), intH);
        }
        for(Tuple t:tuples){
            for(int i=0;i<t.getTupleDesc().numFields();i++){
                if(t.getTupleDesc().getFieldType(i)==Type.INT_TYPE){
                    IntField intV=(IntField) t.getField(i);
                    IntHistorgramMap.get(i).addValue(intV.getValue());
                }
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return f.numPages()*ioCostPerpage;
//        return 0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here

        return (int) Math.ceil(totalTuples()*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return IntHistorgramMap.get(field).avgSelectivity();
//        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if(constant.getType()==Type.INT_TYPE) {
            IntField intConstant=(IntField)constant;
            return  IntHistorgramMap.get(field).estimateSelectivity( op,intConstant.getValue());
        }else{
            StringField stringConstant=(StringField) constant;
            return StringHistogramMap.get(field).estimateSelectivity(op,stringConstant.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here

        return this.numTuples;
//        return 0;
    }

}
