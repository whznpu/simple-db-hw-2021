package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private class Bucket{
        private final int min;
        private final int max;
        private final int id;
        private int size;

        private HashMap<Integer,Integer> map;
        public Bucket(int min,int max,int id){
            this.min=min;
            this.max=max;
            this.id=id;
            this.map= new HashMap<>();
            this.size=0;
        }
        // put a v into the bucket
        public void put(int v){
            if (map.containsKey(v)) {
                map.replace(v, map.get(v), map.get(v) + 1);
            } else {
                map.put(v, 1);
            }
            this.size++;
        }


        public String toString() {
            String res="bucket id:"+this.id+" ";
            for(Map.Entry<Integer,Integer> entry : map.entrySet()){
                res+=" "+entry.getKey();
                int entryValue=entry.getValue();
                assert (entryValue>0);
                while (entryValue>1){
                    res+=" "+entry.getKey();
                    entryValue--;
                }
            }
            return res;
        }
    }

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private final int buckets;
    private final int min;
    private int currentMin;

    private int currentMax;
    private final int max;
    private final int offset;
    private int numTuples;
    // width

    private ArrayList<Bucket> bucketSet;
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets=buckets;
        this.currentMax=min;
        this.currentMin=max;
        this.max=max;
        this.min=min;
        this.bucketSet =new ArrayList<>();
        this.numTuples=0;
        this.offset= (int) Math.ceil((max-min)*1.0/buckets);
        for(int i=0;i<buckets;i++){
            Bucket b=null;
            if(i==buckets-1){
                b= new Bucket(min+i*offset,max,i);
            }else
                b= new Bucket(min+i*offset,min+(i+1)*offset-1,i);
            bucketSet.add(b);
        }
        assert (bucketSet.size()==buckets);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucketId=(v-min)/offset;
        if(v==max) bucketId--;
        bucketSet.get(bucketId).put(v);
        this.currentMax=Math.max(currentMax,v);
        this.currentMin=Math.min(currentMin,v);
        numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        int bucketId=(v-min)/offset;
        if(v==max) bucketId--;


        double selectivityCount=0;
        if(op== Predicate.Op.EQUALS){
            if(v>currentMax||v<currentMin) return 0;
            return (bucketSet.get(bucketId).size)*1.0/offset/numTuples;
        }else if(op== Predicate.Op.NOT_EQUALS){
            return 1-estimateSelectivity(Predicate.Op.EQUALS,v);
        }
        else if(op== Predicate.Op.GREATER_THAN){
            if(v>=max) return 0;
            if(v<=min) return 1;
            if(bucketSet.get(bucketId).size==0){
                selectivityCount=0;
            }else {
                selectivityCount = ((bucketSet.get(bucketId).max-v) * 1.0 / offset) *
                        (bucketSet.get(bucketId).size * 1.0 / numTuples);
            }
            for(int i=bucketId+1;i<buckets;i++){
                selectivityCount+=(bucketSet.get(i).size*1.0)/numTuples;
            }
            return  selectivityCount;
        } else if (op==Predicate.Op.GREATER_THAN_OR_EQ) {
            selectivityCount=estimateSelectivity(Predicate.Op.GREATER_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS,v);
            return selectivityCount>1.0?1.0:selectivityCount;
        } else if(op==Predicate.Op.LESS_THAN){
            return 1-estimateSelectivity(Predicate.Op.GREATER_THAN,v);
        }else{
            return estimateSelectivity(Predicate.Op.LESS_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS,v);
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        int sum=0;
        for(Bucket b:bucketSet){
            sum+=b.size;
        }
        return 1.0*sum/numTuples;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String res="";
        // some code goes here
        for(int i=0;i<buckets;i++){
            res+=bucketSet.get(i).toString();
            res+='\n';
        }
        return null;
    }
}
