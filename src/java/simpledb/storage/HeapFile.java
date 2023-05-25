package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private final TupleDesc td;


    private static class HeapFileIterator implements DbFileIterator{

        private final HeapFile file;

        private final TransactionId tid;




        private int pageId;

        private Iterator<Tuple> it;

        HeapFileIterator(HeapFile file,TransactionId tid){
            this.file=file;
            this.tid=tid;
        }

        public Iterator<Tuple> getTupleIterator(int pageId) throws DbException {
            if(pageId>=0&&pageId< file.numPages()){
                HeapPageId pid=new HeapPageId(file.getId(), pageId);
                HeapPage page=null;
                try{
                    page= (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                }
                catch(TransactionAbortedException e){
                    e.printStackTrace();
                }

                if(page==null) throw  new DbException("get iterator fail! pageNo #" + pageId + "# is invalid!");
                return page.iterator();
            }
            throw  new DbException("get iterator fail! pageNo #" + pageId + "# is invalid!");
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageId=0;
            it=getTupleIterator(pageId);
            while (pageId< file.numPages()&&it==null){
                pageId++;
                if(pageId== file.numPages()){
                    break;
                }
                it=getTupleIterator(pageId);
            }
            if(pageId== file.numPages()){
                it=null;
            }

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it==null) return false;
            if(pageId>= file.numPages()) return false;
            if(!it.hasNext()&& pageId==file.numPages()-1) return false;
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(it==null) throw  new NoSuchElementException("not open");
            if(!it.hasNext()){
                pageId++;
                while (pageId<file.numPages()-1&&null==getTupleIterator(pageId)){
                    pageId++;
                }
                it=getTupleIterator(pageId);
                if(it==null){
                    return  null;
                }
            }
            return it.next();
        }
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it=null;

        }
    }

    private final BufferPool bufferPool;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f=f;
        this.td=td;
        this.bufferPool=Database.getBufferPool();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
//        return null;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here

        int pgNo=pid.getPageNumber();
        int tableId=pid.getTableId();
        int pageSize= BufferPool.getPageSize();
        RandomAccessFile rf=null;
        try{
            rf= new RandomAccessFile(f, "r");
            if((long) pgNo *pageSize>rf.length()){
                throw new IllegalArgumentException("page "+pgNo+" ,table :"+tableId+"is invalid!");
            }
            int offset=pgNo*pageSize;
            rf.seek(offset);
            // 暂且这么写
            byte[] data= new byte[pageSize];
//            for(int i=0;i<pageSize;i++){
//                data[i]=rf.readByte();
//            }
            int read=rf.read(data,0,BufferPool.getPageSize());
            if(read!=BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes not equal to BufferPool.getPageSize() ", tableId, pgNo, read));
            }
            HeapPageId hid=new HeapPageId(tableId,pgNo);
            return new HeapPage(hid,data);
        }catch (IOException e){
            e.printStackTrace();

        }finally {
            if(rf!=null){
                try {
                    rf.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }

        }
        throw new IllegalArgumentException("page "+pgNo+" ,table :"+tableId+"is invalid!");
//        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // Push the specified page to disk.
        // Check whether the page exists
        // if exists, find the seek and Override
        // if not, append the page at the end of the file
        int offset=page.getId().getPageNumber()*BufferPool.getPageSize();
            RandomAccessFile rf=null;
            try{
                rf=new RandomAccessFile(f,"rw");

                rf.seek(offset);
                rf.write(page.getPageData(),0,BufferPool.getPageSize());

            }catch (IOException e){
                e.printStackTrace();
            }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize= BufferPool.getPageSize();
        return (int) Math.floor( f.length()*1.0/pageSize);


//        return 0;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // check existing pages first
        int i=0;
        List<Page> pageList = new ArrayList<>();
        for(i=0;i<numPages();i++){
            HeapPageId pid = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                pageList.add(page);
                return pageList;
            }else{
                Database.getBufferPool().unsafeReleasePage(tid,pid);
            }
        }
        // then adding new page to the file
        Database.getCatalog().addTable(this);
        HeapPageId pid = new HeapPageId(this.getId(), i);

        // directly
        HeapPage page = new HeapPage(pid,HeapPage.createEmptyPageData());
        writePage(page);
        page=(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                pageList.add(page);

                return pageList;
        }
        throw new DbException("The file is full and the tuple can not be added");
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
//        return null;
        ArrayList<Page> pageList = new ArrayList<>();
        final RecordId recordId=t.getRecordId();
        final HeapPageId pageId=(HeapPageId) recordId.getPageId();
        final int pN=recordId.getTupleNumber();
        HeapPage hp= (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
        if(hp!=null) {
            hp.deleteTuple(t);
            hp.markDirty(true,tid);
            pageList.add(hp);
            return pageList;
        }

        throw new DbException("the tuple cannot be deleted or is not a member of the file");
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);

//        return null;
    }


}

