package com.whut.database.backend.DM.pageIndex;

import com.whut.database.backend.DM.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
    页面索引：缓存每一页的空闲空间，提供给上层使用，完成快速插入操作
 */
public class PageIndex {

    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private List<PageInfo>[] lists; // 存储页面索引
    private Lock lock;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for(int i = 0; i <= INTERVALS_NO; i++){
            lists[i] = new ArrayList<>();
        }
    }

    /*
        写完页面后，重新加入页面索引
     */
    public void add(int pgno, int freeSpace){
        lock.lock();
        try{
            int idx = freeSpace / THRESHOLD;
            lists[idx].add(new PageInfo(pgno, freeSpace));
        }finally {
            lock.unlock();
        }
    }

    /*
        获取页面，对于同一个页，不允许并发写操作
     */
    public PageInfo select(int spaceSize){
        lock.lock();
        try{
            int idx = spaceSize / THRESHOLD;
            if (idx < INTERVALS_NO) idx++; // 向上取整
            while (idx <= INTERVALS_NO){
                if (lists[idx].size()  == 0){
                    idx++;
                    continue;
                }
                return lists[idx].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }
    }


}
