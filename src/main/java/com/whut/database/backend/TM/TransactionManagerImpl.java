package com.whut.database.backend.TM;

import com.whut.database.backend.utils.Panic;
import com.whut.database.backend.utils.Parser;
import com.whut.database.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {

    /*
    // XID文件头长度：用来记录管理事务的个数
    public static final int XID_HEADER_LENGTH = 8;

    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，默认为commited状态
    private static final long SUPER_XID = 0;
     */


    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;
    private long xidCount; // 管理的事务个数

    public TransactionManagerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        lock = new ReentrantLock();
        checkXIDCount();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcount，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCount(){
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLen < XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            fc.position(0); // 找到文件的对应位置
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.xidCount = Parser.parseLong(buf.array());

        long end = getXidPosition(xidCount+1);
        if(end != fileLen){
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务xid获取在xid文件中的位置
    private long getXidPosition(long xid){
        // 超级事务不需要记录状态
        return XID_HEADER_LENGTH + (xid-1) * XID_FIELD_SIZE;
    }

    // 开启事务
    @Override
    public long begin() {
        lock.lock();
        try{
            long xid = xidCount + 1;
            updateXID(xid,FIELD_TRAN_ACTIVE);
            incrXIDCount();
            return xid;
        }finally {
            lock.unlock();
        }
    }

    // 更新xid事务的状态
    private void updateXID(long xid, byte status){
        long position = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(position);
            fc.write(buf);
            fc.force(false); // 刷新管道中的数据到磁盘
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 将事务个数+1，并更新文件头
    private void incrXIDCount(){
        xidCount++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCount));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid,FIELD_TRAN_ABORTED);
    }


    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXIDStatus(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXIDStatus(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXIDStatus(xid,FIELD_TRAN_ABORTED);
    }

    // 检查事务状态
    public boolean checkXIDStatus(long xid, byte status){
        long position = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return buf.array()[0] == status;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


}
