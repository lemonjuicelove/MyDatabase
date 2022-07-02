package com.whut.database.backend.DM.page;



import com.whut.database.backend.DM.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page {

    private int pageNumber; // 页号

    private byte[] data; // 页数据

    private boolean dirty; // 脏页标志

    private Lock lock; // 锁

    private PageCache pc; // 页缓存


    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }

}
