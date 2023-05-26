package simpledb.transaction;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.lang.invoke.ConstantCallSite;
import java.lang.management.LockInfo;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


public class LockManager {
    public class LockEntry{
        private ConcurrentHashMap<TransactionId,Lock> owners;
         public LockEntry(){
            this.owners= new ConcurrentHashMap<>();
        }
        private void moveToOwners(Lock l){
            owners.put(l.getTid(),l);
        }
        private boolean checkLock(TransactionId tid){
             return owners.containsKey(tid);
        }

        private boolean removeLock(TransactionId tid){
             boolean flag= false;
             if(owners.remove(tid)!=null){
                flag=true;
             }
            return flag;
        }
    }
     private ConcurrentHashMap<PageId,LockEntry> LockTable;

    public LockManager()
    {
        this.LockTable = new ConcurrentHashMap<>();

    }

    public boolean checkCompatiblity(Lock.LockType t1, Lock.LockType t2){
         return (t1== Lock.LockType.SHARING)&&(t2== Lock.LockType.SHARING);
    }
    public synchronized boolean getLock(TransactionId tid, PageId pid, Lock.LockType lockType) {
        boolean compatiblity=true;
        if(!LockTable.containsKey(pid)){
            // no locks get the lock
            LockEntry e= new LockEntry();
            Lock l= new Lock(tid,lockType);
            // move to the owner immediately
            e.moveToOwners(l);
            LockTable.put(pid,e);
            return true;
        }else{
            // check the page's owner first
            LockEntry e = LockTable.get(pid);
//            assert (e.owners.size()>0);
            if(e.owners.size()==0){
                e.moveToOwners(new Lock(tid, lockType));
                return true;
            }
            for(Map.Entry<TransactionId,Lock> entry:e.owners.entrySet()) {
                if(entry.getKey().equals(tid)){
                    // already get the lock
                    if(lockType== Lock.LockType.SHARING){
                        return true;
                    }else{
                        if(e.owners.size()==1){
                            entry.getValue().updateType();
                            return true;
                        }else{
                            return false;
//                            throw new TransactionAbortedException();
                        }
                    }
                }
                if (checkCompatiblity(entry.getValue().getLockType(), lockType)) {
                    // the lock acquired is compatible with the owner
                }else {
                    compatiblity=false;
                }
            }
            if(compatiblity){
                e.moveToOwners(new Lock(tid,lockType));
                return true;
            }
        }
        return false;
    }

    public synchronized void updateLockType(LockEntry e,TransactionId tid){
        for(Map.Entry<TransactionId,Lock> entry:e.owners.entrySet()) {
            if(!entry.getKey().equals(tid)) {
                e.owners.remove(entry.getKey());
            }else{
                entry.getValue().updateType();;
            }
        }
    }

    public synchronized ConcurrentLinkedDeque<TransactionId> getCurrentOwners(PageId pid){
        ConcurrentLinkedDeque<TransactionId> res=new ConcurrentLinkedDeque<>();

        if(LockTable.containsKey(pid)){
            LockEntry e=LockTable.get(pid);
            for(TransactionId tid:e.owners.keySet()){
                res.add(tid);
            }
        }
        return res;
    }

    public synchronized boolean holdLock(TransactionId tid,PageId pid){
        if(!LockTable.containsKey(pid)){
            return false;
        }else{
            return LockTable.get(pid).checkLock(tid);
        }
    }

    public  synchronized void releaseLock(TransactionId tid,PageId pid){
        // release the pid's lock held by the transaction
        if(LockTable.get(pid).removeLock(tid)){
            this.notifyAll();
        }

    }

    public synchronized ConcurrentLinkedDeque<PageId> getPagesLockedBy(TransactionId tid){
        ConcurrentLinkedDeque<PageId> pageQueue=new ConcurrentLinkedDeque<>();
        for(Map.Entry<PageId,LockEntry> entry:LockTable.entrySet()){
            if(entry.getValue().owners.containsKey(tid)){
                pageQueue.add(entry.getKey());
            }
        }
        return pageQueue;
    }





}
