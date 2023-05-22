package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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
        // some code goes here
        return null;
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
    List<TDItem> TDItemList;
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        assert(typeAr.length==fieldAr.length);
        TDItemList=new ArrayList<>();
        for(int i=0;i<typeAr.length;i++){
            TDItemList.add(new TDItem(typeAr[i],fieldAr[i]));
        }
        // some code goes here
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
        // some code goes here
        TDItemList =new ArrayList<>();
        for(int i=0;i<typeAr.length;i++){
            TDItemList.add(new TDItem(typeAr[i],null));
        }
    }

    public TupleDesc(){
        TDItemList = new ArrayList<>();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItemList.size();
//        return 0;
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
        // some code goes here
        if(i<0|| i>=TDItemList.size()){
            throw new NoSuchElementException("i is not valid filed reference");
        }
        return TDItemList.get(i).fieldName;
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
        // some code goes here
        if(i<0|| i>=TDItemList.size()){
            throw  new NoSuchElementException("i is not a valid field reference");
        }
        return TDItemList.get(i).fieldType;

//        return null;
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
        // some code goes here

        if(name==null){
            throw new NoSuchElementException("no field with a matching name is found");
        }
        for(int i=0;i<TDItemList.size();i++){
            String test1=getFieldName(i);
//            System.out.println(test1);
//            int test=name.compareTo(getFieldName(i));
            if(name.equals(getFieldName(i))){
                return i;
            }
        }
        throw new NoSuchElementException("no field with a matching name is found");

//        return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size=0;
        for(int i=0;i<TDItemList.size();i++){
            size+=getFieldType(i).getLen();
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
        // some code goes here
//        TDItemList = new ArrayList<>();
        if(td1==null){
            return td2;
        }
        if(td2== null){
            return td1;
        }
        TupleDesc td3= new TupleDesc();
        for(int i=0;i<td1.numFields();i++){
            td3.TDItemList.add(td1.TDItemList.get(i));
        }
        for(int i=0;i<td2.numFields();i++){
            td3.TDItemList.add(td2.TDItemList.get(i));
        }
        return td3;
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
        // some code goes here
        if(o instanceof TupleDesc && numFields()==((TupleDesc) o).numFields()){
            for(int i=0;i<numFields();i++){
                if(getFieldName(i)!=((TupleDesc) o).getFieldName(i)|| getFieldType(i)!=((TupleDesc) o).getFieldType(i)){
                    return false;
                }
            }
        }else{
            return false;
        }
        return true;
//        if(numFields())
//        for( type:TDItemList)
//        return false;
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
        // some code goes here
//        String res="";
//        for(TDItem td: TDItemList){
//            res+=td.toString()+" ";
//        }
//        res=res.substring(0,res.length()-1);
////        assert (res.charAt(res.length())==' ');
//        return res;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < TDItemList.size(); i++) {
            TDItem item = TDItemList.get(i);
            builder.append(item.fieldType + "(" + item.fieldName + ")");
            if (i != TDItemList.size() - 1) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }
}
