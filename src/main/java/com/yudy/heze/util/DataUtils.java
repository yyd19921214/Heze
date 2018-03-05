package com.yudy.heze.util;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.zip.CRC32;

public class DataUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataUtils.class);

    public static long calculateChecksum(ByteBuf data, int offset, int length) {
        CRC32 crc32=new CRC32();
        try {
            if (data.hasArray())
                crc32.update(data.array(),data.arrayOffset()+offset,length);
            else {
                for (int i=0;i<length;i++){
                    crc32.update(data.getByte(offset+i));
                }
            }
            return crc32.getValue();

        }finally {
            crc32.reset();
        }
    }

    public static byte[] uint32ToBytes(long value) {
        return uint32ToBytes(ByteOrder.BIG_ENDIAN, value);
    }

    public static byte[] uint32ToBytes(ByteOrder order, long value) {
        byte[] buf = new byte[4];
        uint32ToBytes(order, buf, 0, value);
        return buf;
    }

    private static void uint32ToBytes(ByteOrder order, byte[] buf, int offset, long value) {
        if (offset + 4 > buf.length) throw new IllegalArgumentException("buf no has 4 byte space");
        if (order == ByteOrder.BIG_ENDIAN) {
            buf[offset + 0] = (byte) (0xff & (value >>> 24));
            buf[offset + 1] = (byte) (0xff & (value >>> 16));
            buf[offset + 2] = (byte) (0xff & (value >>> 8));
            buf[offset + 3] = (byte) (0xff & (value));
        } else {
            buf[offset + 0] = (byte) (0xff & (value));
            buf[offset + 1] = (byte) (0xff & (value >>> 8));
            buf[offset + 2] = (byte) (0xff & (value >>> 16));
            buf[offset + 3] = (byte) (0xff & (value >>> 24));
        }
    }









    }
