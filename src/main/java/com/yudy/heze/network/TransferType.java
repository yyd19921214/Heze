package com.yudy.heze.network;

public enum TransferType {

    CALL((byte)1),
    REPLY((byte)2),
    EXCEPTION((byte)3),
    ONEWAY((byte)4);

    public final byte value;

    TransferType(byte value){
        this.value=value;
    }

    final static int size=values().length;

    public static TransferType valueOf(int ordinal){
        if (ordinal < 0 || ordinal >= size) return null;
        return values()[ordinal];
    }

    public static void main(String[] args) {
        System.out.println(TransferType.CALL.value);
        System.out.println(TransferType.values().length);
    }


}
