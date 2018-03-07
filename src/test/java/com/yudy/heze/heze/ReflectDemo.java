package com.yudy.heze.heze;

import java.lang.reflect.Method;

public class ReflectDemo {

    public void A(){
        System.out.println("hello");
    }

    private void B(){
        System.out.println("world");
    }

    public static void main(String[] args) {
        Method[] methods=ReflectDemoChild.class.getDeclaredMethods();
        for(Method m:methods){
            System.out.println(m.getName());
        }
    }


}

class ReflectDemoChild extends ReflectDemo{
    public void C(){
        ;
    }

    private void D(){;}
}


