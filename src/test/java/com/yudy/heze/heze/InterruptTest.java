package com.yudy.heze.heze;

public class InterruptTest {

    public static void main(String[] args) {

        Thread t1=new Thread(
                ()->{
                    System.out.println("enter the thread");
                    try {
//                        Thread.interrupted();
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        System.out.println("enter the segment of exception");
                        System.out.println("interrupt flag is "+Thread.currentThread().isInterrupted());
                        Thread.currentThread().interrupt();
                        System.out.println(Thread.currentThread().isInterrupted());


                    }
                }
        );
        t1.start();
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t1.interrupt();
//        Thread.interrupted();


    }
}


