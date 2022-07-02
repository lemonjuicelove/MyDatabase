package com.whut.database.backend.DM.logger;

import com.google.common.primitives.Bytes;
import com.whut.database.backend.utils.Panic;
import com.whut.database.backend.utils.Parser;
import com.whut.database.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
    日志文件读写

    日志文件标准格式为：
        [XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
        XChecksum是校验和，BadTail是在数据库崩溃时，没有来得及写完的日志数据

    日志格式：
        [Size][Checksum][Data]
        Size是数据长度 Checksum是校验和
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331; // 用于计算校验和

    /*
        前4个字节记录数据长度，后四个字节记录校验和，然后是数据
     */
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position; // 当前日志指针的位置
    private long fileSize; // 文件大小
    private int xCheckSum; // 日志文件校验和


    // 之后每次打开时使用
    public LoggerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    // 首次创建时使用
    public LoggerImpl(RandomAccessFile file, FileChannel fc, int xCheckSum) {
        this.file = file;
        this.fc = fc;
        this.xCheckSum = xCheckSum;
        lock = new ReentrantLock();
    }

    /*
        打开日志文件的时候进行初始化
     */
    @Override
    public void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) Panic.panic(Error.BadLogFileException);

        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xCheckSum = Parser.parseInt(buf.array());

        // 日志文件大小和校验和
        this.xCheckSum = xCheckSum;
        this.fileSize = size;

        checkAndRemoveTail();
    }

    /*
        校验文件和，移除未写完的日志
     */
    private void checkAndRemoveTail(){
        rewind();

        // 校验文件和
        int xCheck = 0;
        while (true){
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calCheckSUm(xCheck,log);
        }

        if (xCheck != xCheckSum) Panic.panic(Error.BadLogFileException);

        // 移除未写完的日志
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        // 文件移到最后的写入位置
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }

        rewind();

    }

    /*
        计算日志的校验和
     */
    private int calCheckSUm(int xCheck, byte[] log){
        for (byte b : log) {
            xCheck = xCheck*SEED + b;
        }
        return xCheck;
    }

    /*
        将日志写入日志文件
     */
    @Override
    public void writeLog(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        updateXCheckSum(log);
    }

    /*
        合并数组：[Size][Checksum][Data]
        行程日志的格式
     */
    private byte[] wrapLog(byte[] data){
        byte[] size = Parser.int2Byte(data.length);
        byte[] checksum = Parser.int2Byte(calCheckSUm(0, data));

        return Bytes.concat(size,checksum,data);
    }

    /*
        更新文件校验和
     */
    private void updateXCheckSum(byte[] log){
        this.xCheckSum = calCheckSUm(this.xCheckSum,log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    /*
        截断文件
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try{
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    /*
        读取日志
     */
    @Override
    public byte[] next() {
        lock.lock();
        try{
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log,OF_DATA,log.length);
        }finally {
            lock.unlock();
        }
    }

    /*
        读取日志
     */
    private byte[] internNext(){

        // 日志已经读完
        if(position + OF_DATA >= fileSize) return null;

        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 日志长度
        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize) return null;

        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checkSum1 = calCheckSUm(0,Arrays.copyOfRange(log,OF_DATA,log.length)); // 计算得到的校验和
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log,OF_CHECKSUM,OF_DATA)); // 日志记录的校验和
        if(checkSum1 != checkSum2) return null;

        position += log.length;

        return log;
    }

    /*
        移动指针到第一条日志的位置
     */
    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }

}
