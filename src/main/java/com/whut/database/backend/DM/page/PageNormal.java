package com.whut.database.backend.DM.page;

import com.whut.database.backend.DM.pageCache.PageCache;
import com.whut.database.backend.utils.Parser;

import java.util.Arrays;

/*
    普通页的管理
    前两个字节表示空闲位置的偏移量
    后面的字节存储数据
    [FreeSpaceOffset] [Data]
 */
public class PageNormal {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /*
        初始化普通页
     */
    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }


    private static void setFSO(byte[] raw, short ofData){
        System.arraycopy(Parser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    /*
        获取数据空闲位置的偏移量
     */
    public static short getFSO(Page pg){
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw){
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    /*
        向页面插入数据
     */
    public static short insert(Page pg, byte[] raw){
        pg.setDirty(true);
        // 获取页面的空闲位置
        short offset = getFSO(pg.getData());
        // 插入数据
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        // 更新空闲位置
        setFSO(pg.getData(),(short)(offset+raw.length));
        return offset;
    }

    /*
        获取页面的空闲空间大小
     */
    public static int getFreeSpace(Page pg){
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /*
        数据库异常崩溃重启后：恢复插入
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,offset+raw.length);

        /*
            如果偏移量小，说明这次插入是有效插入，修改空闲位置
            疑问：如果偏移量大，那是不是不需要此次插入？
         */
        short fso = getFSO(pg.getData());
        if(fso < offset+raw.length){
            setFSO(pg.getData(),(short)(offset+raw.length));
        }
    }

    /*
       数据库异常崩溃重启后：恢复修改
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,offset+raw.length);
    }


}
