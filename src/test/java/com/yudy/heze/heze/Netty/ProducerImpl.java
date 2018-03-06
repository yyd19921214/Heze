package com.yudy.heze.heze.Netty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProducerImpl {

    Lock l = new ReentrantLock();

    Condition empty = l.newCondition();
    Condition full = l.newCondition();

    int capacity = 10;

    List<Integer> list = new ArrayList<>(capacity);

    public void put() {
        try {
            l.lock();
            while (list.size() == capacity) {
                full.await();
            }
            list.add(ThreadLocalRandom.current().nextInt());

            System.out.println(list.size());
            Thread.sleep(ThreadLocalRandom.current().nextInt(1000,2000));
            empty.signalAll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            l.unlock();
        }
    }

    public void get() {
        try {
            l.lock();
            while (list.size() == 0)
                empty.await();
            Thread.sleep(ThreadLocalRandom.current().nextInt(1000,2000));
            list.remove(0);
            System.out.println(list.size());
            full.signalAll();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            l.unlock();
        }


    }

    public static void main(String[] args) throws InterruptedException {
        ProducerImpl pi=new ProducerImpl();
        Thread t0=new Thread(new Producer(pi));
        Thread t1=new Thread(new Consumer(pi));
        Thread t2=new Thread(new Consumer(pi));
//        while (true){
        t0.start();
//        Thread.sleep(2000);
        t1.start();
        t2.start();
//        }
    }

}

class Producer implements Runnable{
    ProducerImpl pi;

    public Producer(ProducerImpl pi){
        this.pi=pi;
    }

    @Override
    public void run() {
        while (true){
            pi.put();
        }

    }
}

class Consumer implements Runnable{
    ProducerImpl pi;
    public Consumer(ProducerImpl pi){
        this.pi=pi;
    }

    @Override
    public void run() {
        while (true){
            pi.get();
        }

    }
}
