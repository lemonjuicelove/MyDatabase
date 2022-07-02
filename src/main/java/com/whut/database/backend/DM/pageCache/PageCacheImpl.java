package com.whut.database.backend.DM.pageCache;

import com.whut.database.backend.DM.page.Page;
import com.whut.database.backend.DM.page.PageImpl;
import com.whut.database.backend.common.AbstractCache;
import com.whut.database.backend.utils.Panic;
import com.whut.database.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/*
    页缓存的实现
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEN_MIN_LIM = 10; // 最小缓存限制

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;
    private AtomicInteger pageNumber;


    public PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResource){
        super(maxResource);
        // 缓存太小
        if (maxResource < MEN_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }

        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();

        // 当前文件已经有的页数
        this.pageNumber = new AtomicInteger((int)length / PAGE_SIZE);
    }



    // 从数据库文件读取页数据
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgNo = (int) key;
        long offset = pageOffset(pgNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }

        return new PageImpl(pgNo,buf.array(),this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        // 是脏页的话 刷新进数据库文件
        if (pg.isDirty()){
            flush(pg);
            pg.setDirty(false);
        }
    }


    // 增加新的数据页
    @Override
    public int newPage(byte[] initData) {
        int pgNo = pageNumber.incrementAndGet();
        Page pg = new PageImpl(pgNo,initData,null);
        flushPage(pg);
        return pgNo;
    }

    // 获取数据页
    @Override
    public Page getPage(int pgno) throws Exception {
        return get(pgno);
    }

    // 关闭缓存
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    // 释放缓存
    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    // 截断该页后面的数据
    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno+1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumber.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumber.intValue();
    }

    // 刷新页
    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    private void flush(Page pg){
        int pageNumber = pg.getPageNumber();
        long offset = pageOffset(pageNumber);

        fileLock.lock();
        try{
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    /*
        获取当前页在文件中的偏移量
        第一页特殊管理
     */
    private static long pageOffset(int pgno){
        return (pgno-1) * PAGE_SIZE;
    }

}
