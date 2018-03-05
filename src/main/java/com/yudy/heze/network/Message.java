package com.yudy.heze.network;

import com.yudy.heze.util.DataUtils;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicInteger;

public class Message {

    private final static AtomicInteger RequestId = new AtomicInteger(1);

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
            byteBuf.writeInt(Message.HEAD_LEN+bodyLength()+Message.CRC_LEN);

            byteBuf.writeShort(getMagic());
            byteBuf.writeByte(getVersion());
            byteBuf.writeByte(getType());
            byteBuf.writeShort(getReqHandlerType());
            byteBuf.writeInt(getSeqId());

            if (bodyLength()>0)
                byteBuf.writeBytes(body);
            long crc32= DataUtils.calculateChecksum(byteBuf,byteBuf.readerIndex()+4,byteBuf.readableBytes()-4);
            byteBuf.writeBytes(DataUtils.uint32ToBytes(crc32));
        }
    }


}
