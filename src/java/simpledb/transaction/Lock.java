package simpledb.transaction;

import java.awt.*;
import java.io.Serializable;

public class Lock {
    private TransactionId tid;
//    LockType
    public enum LockType implements Serializable{
        SHARING,EXCLUSIVE;
        public static LockType getLockType(int i){ return values()[i];};

    @Override
    public String toString() {
        if(this == SHARING){
            return "SHARING";
        }
        if(this ==EXCLUSIVE){
            return "EXCLUSIVE";
        }
        throw new IllegalArgumentException("error LockType");

    }
}
    private LockType lockType;
    public Lock( TransactionId tid, LockType lockType){
        this.tid=tid;
        this.lockType=lockType;
    }
    public TransactionId getTid(){
        return  this.tid;
    }
    public LockType getLockType(){
        return this.lockType;
    }

    public void updateType(){

        if(lockType==LockType.SHARING){
            lockType=LockType.EXCLUSIVE;
        }
    }
}
