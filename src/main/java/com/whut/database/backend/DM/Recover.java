package com.whut.database.backend.DM;

import com.google.common.primitives.Bytes;
import com.whut.database.backend.DM.dataItem.DataItem;
import com.whut.database.backend.DM.logger.Logger;
import com.whut.database.backend.DM.page.Page;
import com.whut.database.backend.DM.page.PageNormal;
import com.whut.database.backend.DM.pageCache.PageCache;
import com.whut.database.backend.TM.TransactionManager;
import com.whut.database.backend.common.SubArray;
import com.whut.database.backend.utils.Panic;
import com.whut.database.backend.utils.Parser;
import com.whut.database.common.Error;

import java.util.*;

/*
    文件恢复策略
 */
public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    private static final int OF_TYPE = 0; // 日志类型
    private static final int OF_XID = OF_TYPE + 1;  // 事务id

    private static final int OF_INSERT_PGNO = OF_XID + 8; // 插入页号
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4; // 页内偏移量
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2; // 插入数据

    private static final int OF_UPDATE_UID = OF_XID + 8; // 修改事务的id
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8; // 修改的数据

    /*
        [LogType] [XID] [Pgno] [Offset] [Raw]
     */
    static class InsertLog{
        long xid; // 事务id
        int pgno; // 页号
        short offset; // 页内偏移量
        byte[] raw; // 插入数据
    }

    /*
        [LogType] [XID] [UID] [OldRaw] [NewRaw]
     */
    static class UpdateLog{
        long xid; // 事务id
        int pgno; // 页号
        short offset; // 页内偏移量
        byte[] oldRaw; // 老数据
        byte[] newRaw; // 新数据
    }

    /*
        恢复文件
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc){
        System.out.println("Recovering");

        lg.rewind();

        int maxPgno = 0;
        while (true){
            byte[] log = lg.next();
            if (log == null) break;
            int pgNo = 0;
            if(isInsertlog(log)){
                InsertLog il = parseInsertLog(log);
                pgNo = il.pgno;
            }else if(isUpdateLog(log)){
                UpdateLog ul = parseUpdateLog(log);
                pgNo = ul.pgno;
            }else{
                Panic.panic(Error.BadLogFileException);
            }

            if (pgNo > maxPgno) maxPgno = pgNo;
        }

        // 只有第一页特殊页
        if (maxPgno == 0) maxPgno = 1;

        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");

    }

    /*
        重做日志操作
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true){
            byte[] log = lg.next();
            if (log == null) return;

            if(isInsertlog(log)){
                InsertLog il = parseInsertLog(log);
                long xid = il.xid;
                if(!tm.isActive(xid)){
                    doInsertLog(pc, log, REDO);
                }
            }else if (isUpdateLog(log)){
                UpdateLog ul = parseUpdateLog(log);
                long xid = ul.xid;
                if(!tm.isActive(xid)){
                    doUpdateLog(pc, log, REDO);
                }
            }else{
                Panic.panic(Error.BadLogFileException);
            }
        }
    }

    /*
        回滚日志操作
     */
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();

        // 将属于某个事务的操作放入同一个集合中
        while (true){
            byte[] log = lg.next();
            if (log == null) break;

            if(isInsertlog(log)){
                InsertLog il = parseInsertLog(log);
                long xid = il.xid;
                if(tm.isActive(xid)){
                   if(!logCache.containsKey(xid)){
                       logCache.put(xid,new ArrayList<>());
                   }
                   logCache.get(xid).add(log);
                }
            }else if (isUpdateLog(log)){
                UpdateLog ul = parseUpdateLog(log);
                long xid = ul.xid;
                if(tm.isActive(xid)){
                    if(!logCache.containsKey(xid)){
                        logCache.put(xid,new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }else{
                Panic.panic(Error.BadLogFileException);
            }
        }

        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            // 倒叙执行回滚日志
            for(int i = logs.size()-1; i >= 0; i--){
                byte[] log = logs.get(i);
                if (isInsertlog(log)){
                    doInsertLog(pc,log,UNDO);
                }else if (isUpdateLog(log)){
                    doUpdateLog(pc,log,UNDO);
                }else {
                    Panic.panic(Error.BadLogFileException);
                }
            }
            // 将事务状态设置为取消
            tm.abort(entry.getKey());
        }

    }

    /*
        执行插入语句
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLog il = parseInsertLog(log);
        Page pg = null;

        try {
            pg = pc.getPage(il.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            if (flag == UNDO){ // 回滚阶段
                DataItem.setInvalid(il.raw);
            }
            PageNormal.recoverInsert(pg,il.raw,il.offset);
        }finally {
            pg.release();
        }
    }

    /*
        执行修改语句
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {

        UpdateLog ul = parseUpdateLog(log);
        Page pg = null;

        try {
            pg = pc.getPage(ul.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            if (flag == REDO){ // 重做阶段
                PageNormal.recoverUpdate(pg,ul.newRaw,ul.offset);
            }else if (flag == UNDO){ // 回滚阶段
                PageNormal.recoverUpdate(pg,ul.oldRaw,ul.offset);
            }else{
                Panic.panic(Error.BadLogFileException);
            }
        }finally {
            pg.release();
        }

    }


    /*
        创建插入日志
     */
    private static InsertLog parseInsertLog(byte[] log) {
        InsertLog il = new InsertLog();
        il.xid = Parser.parseLong(Arrays.copyOfRange(log,OF_XID,OF_INSERT_PGNO));
        il.pgno = Parser.parseInt(Arrays.copyOfRange(log,OF_INSERT_PGNO,OF_INSERT_OFFSET));
        il.offset = Parser.parseShort(Arrays.copyOfRange(log,OF_INSERT_OFFSET,OF_INSERT_RAW));
        il.raw = Arrays.copyOfRange(log,OF_INSERT_RAW,log.length);

        return il;
    }

    /*
        创建修改日志
     */
    private static UpdateLog parseUpdateLog(byte[] log) {
        UpdateLog ul = new UpdateLog();
        ul.xid = Parser.parseLong(Arrays.copyOfRange(log,OF_XID,OF_UPDATE_UID));

        long uid =  Parser.parseLong(Arrays.copyOfRange(log,OF_UPDATE_UID,OF_UPDATE_RAW));
        ul.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        ul.pgno = (int)(uid & ((1L << 32) - 1));

        int length = (log.length - OF_UPDATE_RAW) / 2;
        ul.oldRaw = Arrays.copyOfRange(log,OF_UPDATE_RAW,OF_UPDATE_RAW + length);
        ul.newRaw = Arrays.copyOfRange(log,OF_UPDATE_RAW+length, log.length);

        return ul;
    }

    /*
        创建插入日志
     */
    public static byte[] createInsertLog(long xid, Page pg, byte[] raw){
        byte[] logType = new byte[]{LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgNo = Parser.int2Byte(pg.getPageNumber());
        byte[] offset = Parser.short2Byte(PageNormal.getFSO(pg));
        return Bytes.concat(logType,xidRaw,pgNo,offset,raw);
    }

    /*
        创建修改日志
     */
    public static byte[] updateLog(long xid, DataItem di){
        byte[] logType = new byte[]{LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw,raw.start,raw.end);
        return Bytes.concat(logType,xidRaw,uidRaw,oldRaw,newRaw);
    }

    private static boolean isInsertlog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    private static boolean isUpdateLog(byte[] log) {
        return log[0] == LOG_TYPE_UPDATE;
    }

}
