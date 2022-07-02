package com.whut.database.backend.DM.logger;

import com.whut.database.backend.utils.Panic;
import com.whut.database.backend.utils.Parser;
import com.whut.database.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/*
    日志文件模块
 */
public interface Logger {

    String LOG_SUFFIX = ".log";

    void init();

    void writeLog(byte[] data);

    void truncate(long x) throws Exception;

    byte[] next();

    void rewind();

    void close();

    /*
        创建日志文件
     */
    static Logger create(String path){
        File file = new File(path + LOG_SUFFIX);
        try {
            // 日志文件已经存在
            if (!file.createNewFile()) Panic.panic(Error.FileExistsException);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 日志文件无法读写
        if(!file.canRead() || !file.canWrite()) Panic.panic(Error.FileCannotRWException);

        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file,"rw");
        }catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf,fc,0);
    }

    /*
        打开日志文件
     */
    static Logger open(String path){
        File file = new File(path + LOG_SUFFIX);
        if(!file.exists()) Panic.panic(Error.FileNotExistsException);
        if(!file.canRead() || !file.canWrite()) Panic.panic(Error.FileCannotRWException);

        FileChannel fc = null;
        RandomAccessFile raf = null;

        try{
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        Logger logger =  new LoggerImpl(raf,fc);
        logger.init();

        return logger;
    }

}
