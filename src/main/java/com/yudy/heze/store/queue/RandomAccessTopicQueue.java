package com.yudy.heze.store.queue;

import com.yudy.heze.store.block.RandomAccessBlock;
import com.yudy.heze.store.index.RandomAccessBlockIndex;
import org.apache.commons.lang.ArrayUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class RandomAccessTopicQueue{

    public static final String BLOCK_PREFIX="block";

    public static final String INDEX_PREFIX="index";


    private String queueName; //the topic this queue
    private String fileDir; // file Directory where data and index store
    private RandomAccessBlock writeBlock; //the block which next message should be appended
    private ReentrantLock readLock;
    private ReentrantLock writeLock;
    private long maxOffset;


    public RandomAccessTopicQueue(String queueName,String fileDir){
        this.queueName=queueName;
        this.fileDir=fileDir;
        this.readLock=new ReentrantLock();
        this.writeLock=new ReentrantLock();
        this.writeBlock=getWreteBlockFromDisk(queueName,fileDir);
        this.maxOffset=this.writeBlock.getIndex().getLastOffset();
    }


    public boolean append(byte[] bytes) {
        if (ArrayUtils.isEmpty(bytes))
            return true;
        writeLock.lock();
        try {
            if (!writeBlock.isSpaceAvailable(bytes.length))
                rotateNextWriteBlock();
            long offset=writeBlock.write(bytes);
            this.maxOffset=offset;
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            writeLock.unlock();
        }
    }


    public byte[] read(long offset){
        if(offset>maxOffset){
            return null;
        }
        RandomAccessBlock readBlock;
        byte[] data=null;
        readLock.lock();
        try{
            long firstOffset=searchReadBlockFirstOffset(offset);
            String readIndexName=String.format("%s_%s_%s.umq",INDEX_PREFIX,queueName,String.valueOf(firstOffset));
            if (readIndexName.equals(writeBlock.getIndex().getIndexName())){
                //todo it need to be checked if thread safe
                readBlock=this.writeBlock.duplicate();
                data=readBlock.read(offset);
            }
            else{
                RandomAccessBlockIndex readIndex=new RandomAccessBlockIndex(readIndexName,fileDir);
                readBlock=new RandomAccessBlock(readIndex,fileDir);
                data=readBlock.read(offset);
                readBlock.close();
                readIndex.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            readLock.unlock();
        }

        return data;
    }

    public boolean close(){
        writeLock.lock();
        try{
            writeBlock.close();
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }finally {
            writeLock.unlock();
        }
        return true;
    }


    /**
     * the index name is like: index_{queueName}_{first offset of this index}.umq
     * the block name if like: block_{queueName}_{first offset of this block}.umq
     * @param queueName
     * @param fileDir
     * @return
     */
    private RandomAccessBlock getWreteBlockFromDisk(String queueName,String fileDir){
        File f=new File(fileDir);
        if (!f.isDirectory()){
            throw new IllegalArgumentException("the path: "+fileDir+"is not a valid directory path");
        }
        if (!f.exists()){
            f.mkdirs();
        }
        String[] indexsName=f.list((File dir, String name)->name.startsWith(String.format("%s_%s",INDEX_PREFIX,queueName)));
        if (ArrayUtils.isEmpty(indexsName)){
            //create the first block of this queue
            RandomAccessBlockIndex writeIndex=new RandomAccessBlockIndex(String.format("%s_%s_%s.umq",INDEX_PREFIX,queueName,"0"),fileDir);
            RandomAccessBlock writeBlock = new RandomAccessBlock(writeIndex, fileDir);
            return writeBlock;
        }
        Arrays.sort(indexsName, Comparator.comparingLong((String fileName)->Long.parseLong(getOffsetFromName(fileName))));
        RandomAccessBlockIndex writeIndex=new RandomAccessBlockIndex(indexsName[indexsName.length-1],fileDir);
        //get the block correspond to write index
        RandomAccessBlock writeBlock = new RandomAccessBlock(writeIndex, fileDir);
        return writeBlock;

    }

    //given the file name of index or data, extract the first offset of this index of file
    private String getOffsetFromName(String fileName){
        return fileName.split("_")[2].replaceAll(".umq","");
    }

    private long searchReadBlockFirstOffset(long offset){
        File f=new File(fileDir);
        String[] indexsName=f.list((File dir, String name)->name.startsWith(String.format("%s_%s",INDEX_PREFIX,queueName)));
        List<Long> offsetList=Arrays.stream(indexsName).map(name->Long.parseLong(getOffsetFromName(name))).collect(Collectors.toList());
        offsetList.sort(Comparator.naturalOrder());
        long searchOffset=0;
        for(int i=0;i<offsetList.size();i++){
            if (offsetList.get(i)<offset&&(i==offsetList.size()-1||offsetList.get(i+1)>=offset)){
                searchOffset=offsetList.get(i);
                break;
            }
        }
        return searchOffset;
    }

    private void rotateNextWriteBlock() {
        long offset=this.writeBlock.getIndex().getLastOffset();
        RandomAccessBlockIndex writeIndex=new RandomAccessBlockIndex(String.format("%s_%s_%s.umq",INDEX_PREFIX,queueName,String.valueOf(offset)),fileDir);
        RandomAccessBlock writeBlock = new RandomAccessBlock(writeIndex, fileDir);
        this.writeBlock.sync();
        this.writeBlock.close();
        this.writeBlock=writeBlock;
    }








}
