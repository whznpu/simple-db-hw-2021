package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.*;

import javax.swing.undo.CannotUndoException;
import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private Graph g;

//    LinkedList<Page> pageList;



    private ConcurrentHashMap<PageId,Page> pageCache;


    private ConcurrentLinkedDeque<PageId> pageList;

    private LockManager lockManager;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
//        pageList= new LinkedList<Page>();
        this.numPages=numPages;
        this.pageList=new ConcurrentLinkedDeque<>();
        this.pageCache= new ConcurrentHashMap<>();
        this.lockManager = new LockManager();
        this.g= new Graph();
    }

    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        Lock.LockType lockType=perm==Permissions.READ_ONLY? Lock.LockType.SHARING: Lock.LockType.EXCLUSIVE;
        long st=System.currentTimeMillis();
        boolean isacquired= false;
        isacquired=lockManager.getLock(tid,pid,lockType);
        while(!isacquired){
            long now=System.currentTimeMillis();
//            if(now-st>30){
//                System.out.println("进行判断");
                for(TransactionId owner:lockManager.getCurrentOwners(pid)){
                    g.add_edge(tid,owner);
//                    System.out.println("加边: a:"+tid.getId()+" -> b:"+owner.getId());
                }
                if(!g.topological_sort()) {
                    // must have a circle
                    // abort itself
                    System.out.println("有环解环 abort tid:"+tid.getId()   );
                    g.delete_edge(tid);
                    // 加个backofftime
                    try {
//                        System.out.println("backofftime");
                        TimeUnit.MILLISECONDS.sleep(30);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new TransactionAbortedException();
                }

//            }
            isacquired=lockManager.getLock(tid,pid,lockType);
        }
//        System.out.println("Transaction :"+tid.getId()+" get the "+lockType.toString()+"lock on page:"+pid.getPageNumber());

        if(pageCache.containsKey(pid)){
//            assert (pageCache.size()==pageList.size());
            pageList.remove(pid);
            pageList.offer(pid);
//            assert (pageCache.size()==pageList.size());
            return pageCache.get(pid);
        }else{
            // evict
            if(pageCache.size()==numPages){
                evictPage();
            }
            // get new page from disk
            DbFile file =Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page=file.readPage(pid);
            pageList.offer(pid);
            pageCache.put(pid,page);
            return page;
        }
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdLock(tid,p);
//        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else{
            // when abort
            // release all dirty pages caused by this transaction
            // and read them form the disk
            restorePages(tid);
        }
//        g.delete_edge(tid);
        assert (pageCache.size()==pageList.size());
    }

    public synchronized void restorePages(TransactionId tid){
        ConcurrentLinkedDeque<PageId> pageIds=lockManager.getPagesLockedBy(tid);
        for(PageId pid:pageIds){
            if(pageCache.containsKey(pid)){
                DbFile file =Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page p=file.readPage(pid);
                pageCache.put(pid,p);
                lockManager.releaseLock(tid,pid);
                pageList.remove(pid);
                pageList.addFirst(pid);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // get the table's page according to the tableId
        DbFile DbFile= Database.getCatalog().getDatabaseFile(tableId);
        if(DbFile==null){
            throw new IOException("DbFile error");
        }
        List<Page> pageDirtyList=DbFile.insertTuple(tid,t);
        for(Page p:pageDirtyList){
            // 这最好的办法还是做一个lrucache 我当时没做 通过这个管理比较好
            p.markDirty(true,tid);
//            if(pageCache.size()==numPages){
//                evictPage();
//            }
            pageCache.put(p.getId(),p);
        }
//        System.out.println("Transaction:"+tid.getId()+" insert "+t.toString()+" into table:"+tableId);
        // not on the disk, but on the bufferpool
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
//        DbFile DbFile=Database.getCatalog().getDatabaseFile();
        final int tableId=t.getRecordId().getPageId().getTableId();

        DbFile DbFile=Database.getCatalog().getDatabaseFile(tableId);
//        assert (pageCache.size()==pageList.size());
        List<Page> pageDirtyList=DbFile.deleteTuple(tid,t);
        for(Page p:pageDirtyList){
            p.markDirty(true,tid);
//            if(pageCache.size()==numPages){
//                evictPage();
//            }
            pageCache.put(p.getId(),p);
        }
//        System.out.println("Transaction:"+tid.getId()+" delete "+t.toString());
        // some code goes here

        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Map.Entry<PageId,Page> entry:pageCache.entrySet()){
            pageList.remove(entry.getKey());
            DbFile DbFile=Database.getCatalog().getDatabaseFile(entry.getValue().getId().getTableId());
            DbFile.writePage(entry.getValue());
            pageCache.remove(entry.getKey());

        }
//        assert (pageList.size()==0);

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pageCache.remove(pid);
        pageList.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if(pageCache.containsKey(pid)){
            Page p=pageCache.get(pid);
            pageList.remove(pid);
            pageCache.remove(pid);
            DbFile dbFile=Database.getCatalog().getDatabaseFile(pid.getTableId());
//            System.out.println("page "+p.getId()+" is flushed");
            dbFile.writePage(p);
            p.markDirty(false,null);
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // get all pages locked by the transaction
        ConcurrentLinkedDeque<PageId> pageQueue= new ConcurrentLinkedDeque<>();
        pageQueue=lockManager.getPagesLockedBy(tid);

        for (PageId pid:pageQueue) {
//            System.out.println("Transaction" +tid.getId()+" flush page:"+pid.getPageNumber());
            flushPage(pid);
            lockManager.releaseLock(tid,pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        for(PageId vicim:pageList){
            if(pageCache.get(vicim).isDirty()==null){
                discardPage(vicim);
                return;
            }else{
                pageList.remove(vicim);
                pageList.addFirst(vicim);
            }
        }
        throw  new DbException("All dirty");
    }

}
