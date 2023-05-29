package simpledb.storage;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


public class Graph {
    int vNum;
    HashMap<TransactionId,
            LinkedList<TransactionId>> adj;
   LinkedList<TransactionId> q;
    HashMap<TransactionId,Integer> indegree;
    public Graph(){
        this.vNum=0;
        this.adj= new HashMap<>();
        this.q= new LinkedList<>();
        this.indegree=new HashMap<>();
    }
    public synchronized void add_edge(TransactionId v,TransactionId w){
        if(v.equals(w)){
            return;
        }
        if(adj.containsKey(v)){
            adj.get(v).add(w);
            if (indegree.containsKey(w)) {
                indegree.put(w, indegree.get(w) + 1);
            } else {
                vNum++;
                indegree.put(w, 1);
            }
        }else{
            vNum++;
            LinkedList list= new LinkedList();
            list.add(w);
            adj.put(v,list);
            if (indegree.containsKey(w)) {
                indegree.put(w, indegree.get(w) + 1);
            } else {
                vNum++;
                indegree.put(w, 1);
            }
        }
    }
    public synchronized void delete_edge(TransactionId v){
        vNum--;
        adj.remove(v);
    }
    // 还有本身的环

    public synchronized boolean topological_sort(){
        for(TransactionId tid:adj.keySet()){
            if(indegree.get(tid)==null||indegree.get(tid)==0){
                q.add(tid);
            }
        }
        int count=0;
        while(!q.isEmpty()){
            TransactionId ctid=q.getFirst();
            q.pop();
            count++;
            if(adj.get(ctid)!=null){
                for (TransactionId tid : adj.get(ctid)) {
                    indegree.put(tid, indegree.get(tid) - 1);
                    if (indegree.get(tid) == 0) {
                        q.add(tid);
                    }
                }
            }
        }

//        System.out.println("vNum:"+vNum+" count:"+count);
        return count<vNum? false:true;
    }

}
