package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     * @param returnTuple
     *              a single tuple with one integer field
     */
    private  final TransactionId t;
    private OpIterator child;
    private final int tableId;
    private int dirtyNums=0;
    private boolean hasNextFlag;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t=t;
        this.child=child;
        this.tableId=tableId;
        this.dirtyNums=0;
        this.hasNextFlag=true;


    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
//        return null;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        // put the insert at open
        child.open();
        super.open();

    }

    public void close() {
        // some code goes here
        super.close();
        child.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
//        return null;
        if(!hasNextFlag)
            return null;
        hasNextFlag=false;
        while (child.hasNext()){
            Tuple tuple=child.next();
            try {
                dirtyNums++;
                Database.getBufferPool().insertTuple(t,tableId,tuple);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Tuple dirtyTupleNums = new Tuple(getTupleDesc());
        dirtyTupleNums.setField(0,new IntField(dirtyNums));
        return dirtyTupleNums;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
//        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if(this.child!=children[0]){
            this.child=children[0];
        }
    }
}
