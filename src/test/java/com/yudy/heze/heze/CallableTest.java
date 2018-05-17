package com.yudy.heze.heze;

import java.util.Random;
import java.util.concurrent.*;

public class CallableTest {


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Long l=4L;
        System.out.println(Long.BYTES);
//        String s="0000000056";
//        System.out.println(Integer.parseInt(s));

//        Thread t=new Thread(()->{
//            int i=0;
//           while (i++<10000){
//               System.out.println("hello");
//           }
//        });
//        t.start();
//        Thread.currentThread().interrupt();
//        t.join(1000);
//
//        System.out.println("world");

//        t.interrupt();
//        t.join(1000);

//        FutureTask<Integer> f=new FutureTask<Integer>(new CallableDemo());
//        ExecutorService exe = Executors.newFixedThreadPool(2);
//        exe.submit(f);
//        exe.shutdown();
//        exe.awaitTermination(Integer.MAX_VALUE,TimeUnit.MILLISECONDS);
//        System.out.println(f.get());

    }



}


class CallableDemo implements Callable<Integer>{
    @Override
    public Integer call() throws Exception {
        return new Random().nextInt();
    }
}


