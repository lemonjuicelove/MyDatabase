package com.whut.database.backend.TM;


import com.whut.database.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.whut.database.common.Error;

/*
    事务管理器
 */
public interface TransactionManager {

    // XID文件头长度：用来记录管理事务的个数
    int XID_HEADER_LENGTH = 8;

    // 每个事务的占用长度
    int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    byte FIELD_TRAN_ACTIVE = 0;
    byte FIELD_TRAN_COMMITTED = 1;
    byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，默认为commited状态
    long SUPER_XID = 0;

    // 文件的后缀名
    String XID_SUFFIX = ".xid";

    long begin(); // 开启事务

    void commit(long xid); // 提交事务

    void abort(long xid); // 取消事务

    void close(); // 关闭事务

    boolean isActive(long xid); // 检查事务是否在进行状态

    boolean isCommitted(long xid); // 检查事务是否在提交状态

    boolean isAborted(long xid); // 检查事务是否在取消状态


    // 创建事务管理器
    static TransactionManager create(String path){
        File file = new File(path + XID_SUFFIX);
        try {
            // 数据库已经存在
            if(!file.createNewFile()) Panic.panic(Error.FileExistsException);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 检查文件权限
        if(!file.canRead() || !file.canWrite()) Panic.panic(Error.FileCannotRWException);

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try{
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new TransactionManagerImpl(raf,fc);
    }

    // 打开事务管理器
    static TransactionManager open(String path){
        File file = new File(path + XID_SUFFIX);
        if(!file.exists()) Panic.panic(Error.FileNotExistsException);
        if(!file.canRead() || !file.canWrite()) Panic.panic(Error.FileCannotRWException);

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try{
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf,fc);
    }


}
