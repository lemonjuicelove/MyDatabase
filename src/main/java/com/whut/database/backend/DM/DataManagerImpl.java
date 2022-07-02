package com.whut.database.backend.DM;

import com.whut.database.backend.DM.dataItem.DataItem;
import com.whut.database.backend.DM.dataItem.DataItemImpl;
import com.whut.database.backend.DM.logger.Logger;
import com.whut.database.backend.DM.page.Page;
import com.whut.database.backend.DM.page.PageNormal;
import com.whut.database.backend.DM.page.PageOne;
import com.whut.database.backend.DM.pageCache.PageCache;
import com.whut.database.backend.DM.pageIndex.PageIndex;
import com.whut.database.backend.DM.pageIndex.PageInfo;
import com.whut.database.backend.TM.TransactionManager;
import com.whut.database.backend.common.AbstractCache;
import com.whut.database.backend.utils.Panic;
import com.whut.database.backend.utils.Types;
import com.whut.database.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(TransactionManager tm, PageCache pc, Logger logger) {
        super(0);
        this.tm = tm;
        this.pc = pc;
        this.logger = logger;
        this.pageIndex = new PageIndex();
    }

    /*
        初始化第一页特殊页
     */
    void initPageOne(){
        int pgno = pc.newPage(PageOne.initRaw()); // 第一页
        assert pgno == 1;
        try {
            // 获取第一页的缓存
            pageOne = pc.getPage(pgno);
        }catch (Exception e){
            Panic.panic(e);
        }

        pc.flushPage(pageOne);
    }

    /*
        检查数据库是否正常关闭
     */
    boolean loadCheckPageOne(){
        try {
            pageOne = pc.getPage(1);
        }catch (Exception e){
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /*
        初始化页索引
     */
    void fillPageIndex(){
        int pageNumber = pc.getPageNumber();
        // 第一页不加入页面索引
        for(int i = 2; i <= pageNumber; i++){
            Page pg = null;
            try{
                pg = pc.getPage(i); // 引用计数+1
            }catch (Exception e){
                Panic.panic(e);
            }

            // 将该页加入到页面索引
            pageIndex.add(pg.getPageNumber(), PageNormal.getFreeSpace(pg));
            pg.release(); // 释放缓存，引用计数-1
        }
    }


    /*
        向下读数据
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if(!di.isValid()){
            di.release();
            return null;
        }
        return di;
    }

    /*
        向下写入数据：获取能够存储内容的页面，先写入日志，再插入数据
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 数据过大
        if (raw.length > PageNormal.MAX_FREE_SPACE) throw Error.DataTooLargeException;

        PageInfo pi = null;
        for(int i = 0; i < 5; i++){
            pi = pageIndex.select(raw.length);
            if (pi != null){ // 获取到了能够存放数据的缓存页
                break;
            }else{
                int newPgno = pc.newPage(PageNormal.initRaw()); // 增加新的缓存页
                pageIndex.add(newPgno,PageNormal.MAX_FREE_SPACE); // 将新的缓存页加入页面索引
            }
        }

        if (pi == null) throw Error.DatabaseBusyException;

        Page pg = null;
        int freeSpace = 0;
        try{
            pg = pc.getPage(pi.pgno); // 获取数据页
            byte[] log = Recover.createInsertLog(xid, pg, raw);
            logger.writeLog(log); // 写入日志文件
            short offset = PageNormal.insert(pg, raw); // 向缓存页插入数据
            pg.release(); // 释放缓存

            return Types.addressToUid(pi.pgno,offset);
        }finally {
            // 将写好的数据页重新加入到页面缓存中
            if (pg != null){
                pageIndex.add(pi.pgno,PageNormal.getFreeSpace(pg));
            }else {
                pageIndex.add(pi.pgno,freeSpace);
            }
        }
    }

    /*
        关闭数据库
     */
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /*
        获取缓存
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>= 32;
        int pgNo = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgNo);

        return DataItem.parseDataItem(pg,offset,this);
    }

    /*
        释放缓存
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    /*
        为xid生成修改日志
     */
    public void logDataItem(long xid, DataItem di){
        byte[] log = Recover.updateLog(xid, di);
        logger.writeLog(log);
    }

    /*
        释放缓存
     */
    public void releaseDataItem(DataItem di){
        super.release(di.getUid());
    }

}
