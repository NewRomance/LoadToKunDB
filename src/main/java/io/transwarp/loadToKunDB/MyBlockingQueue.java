package io.transwarp.loadToKunDB;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MyBlockingQueue<T>{
    AtomicInteger empty;
    AtomicInteger full;
    AtomicInteger mutex;
    ArrayList<T> blockingQueue;

    public MyBlockingQueue(int size){
        mutex = new AtomicInteger(1);
        this.empty = new AtomicInteger(size);
        this.full = new AtomicInteger(0);
        blockingQueue = new ArrayList<T>();
    }

    public T take(){
        T obj = null;
        if (full.decrementAndGet() > 0){
            obj = blockingQueue.remove(0);
            empty.incrementAndGet();
        }
        return obj;
    }

    public boolean put(T obj){
        boolean flag = false;
        if (empty.decrementAndGet() > 0){
            flag = blockingQueue.add(obj);
            full.incrementAndGet();
        }
        return flag;
    }
}
