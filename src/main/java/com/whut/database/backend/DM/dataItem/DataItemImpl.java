package com.whut.database.backend.DM.dataItem;

import com.whut.database.backend.DM.DataManagerImpl;
import com.whut.database.backend.DM.page.Page;
import com.whut.database.backend.common.SubArray;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
    DataItem结构
        [ValidFlag] [DataSize] [Data]
        ValidFlag用来记录是否有效：0无效，1有效

    作用：给上层提供的数据抽象
    上层执行流程：在修改之前需要调用 before() 方法，想要撤销修改时，调用 unBefore() 方法，在修改完成后，调用 after() 方法
 */
public class DataItemImpl implements DataItem {

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock; // 读锁
    private Lock wLock; // 写锁
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, DataManagerImpl dm, long uid, Page pg) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
    }

    /*
        验证数据是否有效
     */
    public boolean isValid(){
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw,raw.start + OF_DATA,raw.end);
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }

    /*
        修改之前调用：将未修改的数据保存起来
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw,raw.start,oldRaw,0,oldRaw.length);
    }

    /*
        撤销修改时调用：恢复原来的数据
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw,0,raw.raw,raw.start,oldRaw.length);
    }

    /*
        完成修改时调用：将操作写入日志
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid,this);
        wLock.unlock();
    }

    /*
        释放引用的缓存
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

}
