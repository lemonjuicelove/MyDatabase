package com.whut.database.backend.VM;

import com.google.common.primitives.Bytes;
import com.whut.database.backend.DM.dataItem.DataItem;
import com.whut.database.backend.common.SubArray;
import com.whut.database.backend.utils.Parser;

import java.util.Arrays;

/*
 VM向上层抽象出entry记录
    entry结构：[XMIN] [XMAX] [data]
    XMIN 是创建该条记录（版本）的事务编号，而 XMAX 则是删除该条记录（版本）的事务编号
 */
public class Entry {

    private static final int OF_X_MIN = 0;
    private static final int OF_XMAX = OF_X_MIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem; // 数据
    private VersionManager vm; // 版本控制器


    public static Entry newEntry(long uid, DataItem di,VersionManager vm){
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = di;
        entry.vm = vm;
        return entry;
    }

    /*
        生成一条版本记录
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception{
        DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
        return newEntry(uid,di,vm);
    }

    /*
        生成记录
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data){
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin,xmax,data);
    }

    /*
        释放记录的引用
     */
    public void release(){
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove(){
        dataItem.release();
    }

    /*
        获取版本记录中的数据
     */
    public byte[] data(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw,sa.start + OF_DATA,data,0,data.length);
            return data;
        }finally {
            dataItem.rUnLock();
        }
    }

    /*
        获取低水位
     */
    public long getXmin(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start + OF_X_MIN, sa.start + OF_XMAX));
        }finally {
            dataItem.rUnLock();
        }
    }

    /*
        获取高水位
     */
    public long getXmax(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start + OF_XMAX, sa.start + OF_DATA));
        }finally {
            dataItem.rUnLock();
        }
    }

    /*
        设置高水位
     */
    public void setXmax(long xid){
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid),0,sa.raw,sa.start+OF_XMAX,8);
        }finally {
            dataItem.after(xid);
        }
    }


    public long getUid(){
        return uid;
    }


}
