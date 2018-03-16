package com.yudy.heze.heze;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrencyTest {

    Lock lock = new ReentrantLock();
    Condition notFull = lock.newCondition();
    Condition notEmpty = lock.newCondition();
    List<Integer> queue = new ArrayList<>(10);
    Random rnd = new Random(1);
    volatile boolean produceFlag = true;
    volatile boolean consumeFlag = true;

    private static int QUEUE_SIZE = 10;

    class IntProducer implements Runnable {
        @Override
        public void run() {
            while (true && produceFlag) {
                try {
                    lock.lock();
                    while (queue.size() == QUEUE_SIZE) {
                        notFull.await();
                    }
                    queue.add(1);
                    System.out.println(String.format("Thread %s add en element into queue size is %d", Thread.currentThread().getName(), queue.size()));
                    notEmpty.signalAll();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }

            }
        }
    }

    class IntConsumer implements Runnable {
        @Override
        public void run() {
            while (true && consumeFlag) {
                try {
                    lock.lock();
                    while (queue.isEmpty())
                        notEmpty.await();
                    queue.remove(0);
                    System.out.println(String.format("Thread %s remove en element into queue size is %d", Thread.currentThread().getName(), queue.size()));
                    notFull.signalAll();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
                try {
                    Thread.sleep(rnd.nextInt(50));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ConcurrencyTest test = new ConcurrencyTest();

        Thread t1 = new Thread(test.new IntProducer());
        t1.setName("t1 producer");
        t1.start();
        Thread.sleep(1000);
        test.produceFlag=false;


        Thread t2=new Thread(test.new IntConsumer());
        t2.setName("t2 consumer");
//        Thread t3=new Thread(test.new IntConsumer());
//        t3.setName("t3 consumer");
//        Thread t4=new Thread(test.new IntConsumer());
//        t4.setName("t4 consumer");

//        Thread disp=new Thread(()->{
//            System.out.println("queue size is: "+test.queue.size());
//            try {
//                Thread.sleep(200);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//
//        disp.start();
        t2.start();
//        t3.start();
//        t4.start();


    }

}
