package com.yudy.heze.network;

import com.yudy.heze.util.DataUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.concurrent.atomic.AtomicInteger;

public class Message {

    private final static AtomicInteger RequestId = new AtomicInteger(1);

    //表示整个message长度的字段的长度,需要在包处理时跳过
    public final static int FRAME_LEN = 4;

    public final static int MAGIC = 0xf11f;

    public final static int HEAD_LEN = 2 + 1 + 1 + 2 + 4;

    public final static int CRC_LEN = 4;

    public final static int BODY_MAX_LEN = 8388607 - HEAD_LEN - CRC_LEN;

    private short magic = (short) MAGIC;

    private byte version = (byte) 10;

    private byte type = 0;  // CALL | REPLY | EXCEPTION

    private short reqHandlerType = 0;

    private int seqId = 0;

    private transient byte[] body = new byte[0];

    public static Message newRequestMessage() {
        Message msg = new Message();
        msg.type = TransferType.CALL.value;
        msg.seqId = RequestId.getAndIncrement();
        return msg;
    }

    public static Message newExceptionMessage() {
        Message msg = new Message();
        msg.type = TransferType.EXCEPTION.value;
        return msg;
    }

    public static Message newResponseMessage() {
        Message msg = new Message();
        msg.setType(TransferType.REPLY.value);
        return msg;
    }

    public short getMagic() {
        return magic;
    }

    public void setMagic(short magic) {
        this.magic = magic;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public short getReqHandlerType() {
        return reqHandlerType;
    }

    public void setReqHandlerType(short reqHandlerType) {
        this.reqHandlerType = reqHandlerType;
    }

    public int getSeqId() {
        return seqId;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int bodyLength() {
        return body == null ? 0 : body.length;
    }

    public int serializedSize() {
        return HEAD_LEN + bodyLength() + CRC_LEN;
    }

    public void writeToByteBuf(ByteBuf byteBuf) {
        if (byteBuf != null) {
            byteBuf.writeInt(serializedSize());
            byteBuf.writeShort(getMagic());
            byteBuf.writeByte(getVersion());
            byteBuf.writeByte(getType());
            byteBuf.writeShort(getReqHandlerType());
            byteBuf.writeInt(getSeqId());
            if (bodyLength() > 0)
                byteBuf.writeBytes(body);
            long crc32 = DataUtils.calculateChecksum(byteBuf, byteBuf.readerIndex() + 4, byteBuf.readableBytes() - 4);
            byteBuf.writeBytes(DataUtils.uint32ToBytes(crc32));
        }
    }

    public void readFromByteBuf(ByteBuf byteBuf) {
        if (byteBuf != null) {

            int len = byteBuf.readableBytes() - CRC_LEN;
            long crc32 = DataUtils.calculateChecksum(byteBuf, byteBuf.readerIndex(), len);
            int magic = byteBuf.readUnsignedShort();
            if (magic != MAGIC) {
                byteBuf.discardReadBytes();
                byteBuf.release();
                throw new RuntimeException("协议magic无效");
            }

            byte version = byteBuf.readByte();
            byte type = byteBuf.readByte();
            short reqHandlerType = byteBuf.readShort();
            int seqId = byteBuf.readInt();

            int body_len = len - HEAD_LEN;
            byte[] body = new byte[body_len];
            byteBuf.readBytes(body);

            long read_crc32 = byteBuf.readUnsignedInt();
            if (read_crc32 != crc32) {
                byteBuf.discardReadBytes();
                byteBuf.release();
                throw new RuntimeException("CRC32不对");
            }

            this.setVersion(version);
            this.setType(type);
            this.setSeqId(seqId);
            this.setReqHandlerType(reqHandlerType);
            this.setBody(body);

            byteBuf.release();

        }
    }

    public static Message buildFromByteBuf(ByteBuf byteBuf) {
        Message msg = new Message();
        msg.readFromByteBuf(byteBuf);
        return msg;
    }

    @Override
    public String toString() {
        return String.format("magic:%s, version:%s, type:%s, reqHandlerType:%s, seqId:%d", magic, version, type, reqHandlerType, seqId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Message) {
            Message m = (Message) obj;
            return getMagic() == m.getMagic()//
                    && getVersion() == m.getVersion()//
                    && getType() == m.getType()//
                    && getReqHandlerType() == m.getReqHandlerType()//
                    && getSeqId() == m.getSeqId()
                    && bodyLength() == m.bodyLength();
        }
        return false;
    }


    public static void main(String[] args) {
        Message msg = newRequestMessage();
        msg.body = "abcd".getBytes();
        System.out.println(msg.seqId);
        System.out.println(msg.body[0]);
        ByteBuf inBuf = Unpooled.buffer();
        msg.writeToByteBuf(inBuf);
        inBuf.skipBytes(FRAME_LEN);
        Message msg2 = newRequestMessage();
        msg2.readFromByteBuf(inBuf);
    }


}
