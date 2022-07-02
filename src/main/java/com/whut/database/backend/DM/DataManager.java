package com.whut.database.backend.DM;

import com.whut.database.backend.DM.dataItem.DataItem;
import com.whut.database.backend.DM.logger.Logger;
import com.whut.database.backend.DM.page.PageNormal;
import com.whut.database.backend.DM.page.PageOne;
import com.whut.database.backend.DM.pageCache.PageCache;
import com.whut.database.backend.DM.pageCache.PageCacheImpl;
import com.whut.database.backend.TM.TransactionManager;

/*
    数据管理模块
 */
public interface DataManager {

    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /*
        创建DM
     */
    static DataManager create(String path, long mem, TransactionManager tm){
        PageCache pc = PageCache.create(path,mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(tm,pc,lg);
        dm.initPageOne(); // 初始化第一页

        return dm;
    }

    /*
        打开DM
     */
    static DataManager open(String path, long mem, TransactionManager tm){
        PageCacheImpl pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);

        DataManagerImpl dm = new DataManagerImpl(tm,pc,lg);
        // 检查数据库是否正常关闭
        if(!dm.loadCheckPageOne()){
            Recover.recover(tm,lg,pc);
        }

        dm.fillPageIndex(); // 初始化页索引
        PageOne.setVcOpen(dm.pageOne); // 写入校验的随机字节
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }

}
