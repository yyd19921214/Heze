package com.yudy.heze.heze;

import java.util.Random;
import java.util.concurrent.*;

public class CallableTest {


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        FutureTask<Integer> f=new FutureTask<Integer>(new CallableDemo());
        ExecutorService exe = Executors.newFixedThreadPool(2);
        exe.submit(f);
        exe.shutdown();
        exe.awaitTermination(Integer.MAX_VALUE,TimeUnit.MILLISECONDS);
        System.out.println(f.get());

    }



}


class CallableDemo implements Callable<Integer>{
    @Override
    public Integer call() throws Exception {
        return new Random().nextInt();
    }
}


