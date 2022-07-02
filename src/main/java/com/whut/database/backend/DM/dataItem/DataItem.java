package com.whut.database.backend.DM.dataItem;

import com.google.common.primitives.Bytes;
import com.whut.database.backend.DM.DataManager;
import com.whut.database.backend.DM.DataManagerImpl;
import com.whut.database.backend.DM.page.Page;
import com.whut.database.backend.common.SubArray;
import com.whut.database.backend.utils.Parser;
import com.whut.database.backend.utils.Types;

import java.util.Arrays;

public interface DataItem {

    int OF_VALID = 0; // 校验标志
    int OF_SIZE = 1; // 数据长度
    int OF_DATA = 3; // 数据

    SubArray data();
    SubArray getRaw();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();

    /*
        生成DataItem
     */
    static byte[] wrapDataItemRaw(byte[] raw){
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid,size,raw);
    }


    /*
        将页面数据转化为DataItem
     */
    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm){
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw,offset + OF_SIZE,offset + OF_DATA));
        short length = (short)(size + OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(),offset);
        return new DataItemImpl(new SubArray(raw,offset,offset+length),new byte[length],dm,uid,pg);
    }

    /*
        设置有效性
     */
    static void setInvalid(byte[] raw){
        raw[OF_VALID] = (byte)1;
    }

}
