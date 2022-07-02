package com.whut.database.backend.DM.pageCache;


import com.whut.database.backend.DM.page.Page;
import com.whut.database.backend.utils.Panic;
import com.whut.database.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/*
    页缓存
 */
public interface PageCache {


    // 页大小：8kb
    int PAGE_SIZE = 1 << 13;

    String DB_SUFFIX = ".db";

    int newPage(byte[] initData);

    Page getPage(int pgno) throws Exception;

    void close();

    void release(Page page); // 释放页

    void truncateByBgno(int maxPgno);

    int getPageNumber();

    void flushPage(Page pg); // 刷新页


    /*
        创建数据库缓存
     */
    static PageCacheImpl create(String path,long memory){
        File file = new File(path + DB_SUFFIX);
        try {
            if(!file.createNewFile()) Panic.panic(Error.FileExistsException);
        }catch (Exception e){
            Panic.panic(e);
        }
        if (!file.canRead() || ! file.canWrite()) Panic.panic(Error.FileCannotRWException);

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf,fc,(int)(memory / PAGE_SIZE));
    }

    /*
        打开数据库缓存
     */
    static PageCacheImpl open(String path, long memory){
        File file = new File(path + DB_SUFFIX);
        if(!file.exists()) Panic.panic(Error.FileNotExistsException);
        if (!file.canRead() || ! file.canWrite()) Panic.panic(Error.FileCannotRWException);

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf,fc,(int)(memory / PAGE_SIZE));
    }

}
