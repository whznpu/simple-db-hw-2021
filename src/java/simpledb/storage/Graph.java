package simpledb.storage;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


public class Graph {
    int vNum;
    ConcurrentHashMap<TransactionId,
            ConcurrentLinkedDeque<TransactionId>> adj;
    ConcurrentLinkedDeque<TransactionId> q;
    ConcurrentHashMap<TransactionId,Integer> indegree;
    public Graph(){
        this.vNum=0;
        this.adj= new ConcurrentHashMap<>();
        this.q= new ConcurrentLinkedDeque<>();
        this.indegree=new ConcurrentHashMap<>();
    }
    public synchronized void add_edge(TransactionId v,TransactionId w){
        if(adj.containsKey(v)){
            adj.get(v).add(w);
            if (indegree.containsKey(w)) {
                indegree.put(w, indegree.get(w) + 1);
            } else {
                indegree.put(w, 1);
            }
        }else{
            vNum++;
            ConcurrentLinkedDeque list= new ConcurrentLinkedDeque<>();
            list.add(w);
            adj.put(v,list);
            indegree.put(w,1);
        }
    }
    public synchronized void delete_edge(TransactionId v){
        vNum--;
        adj.remove(v);
    }

    public synchronized boolean topological_sort(){
        for(TransactionId tid:adj.keySet()){
            if(indegree.get(tid)==0){
                q.add(tid);
            }
        }
        int count=0;
        while(!q.isEmpty()){
            TransactionId ctid=q.getFirst();
            q.pop();
            count++;
            for(TransactionId tid:adj.get(ctid)){
                if(indegree.get(tid)==1){
                    q.add(tid);
                    indegree.put(tid,indegree.get(tid)-1);
                }
            }
        }
//        System.out.println("vNum:"+vNum+" count:"+count);
        return count<vNum? false:true;
    }

}
