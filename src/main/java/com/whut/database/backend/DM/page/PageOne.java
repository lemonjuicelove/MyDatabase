package com.whut.database.backend.DM.page;

import com.whut.database.backend.DM.pageCache.PageCache;
import com.whut.database.backend.utils.RandomUtil;

import java.util.Arrays;

/*
    第一页特殊管理：验证数据库是否正常关闭

    db启动时给100~107字节处填入一串随机字节，db关闭时将其拷贝到108~115字节
    下一次启动时，如果相同的话，说明是正常关闭，否则，进入数据恢复过程
 */
public class PageOne {

    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    /*
        数据库启动时，就设置初始字节
     */
    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg){
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }


    public static void setVcOpen(byte[] raw){
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OF_VC,LEN_VC);
    }

    /*
      数据库关闭时，将随机值拷贝到108~115
     */
    public static void setVcClose(Page pg){
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    public static void setVcClose(byte[] raw){
        System.arraycopy(raw,OF_VC,raw,OF_VC+LEN_VC,LEN_VC);
    }


    /*
        检查数据库是否正常关闭
     */
    public static boolean checkVc(Page pg){
        return checkVc(pg.getData());
    }

    public static boolean checkVc(byte[] raw){
        byte[] start = Arrays.copyOfRange(raw, OF_VC, LEN_VC);
        byte[] end = Arrays.copyOfRange(raw, OF_VC + LEN_VC, LEN_VC);
        return Arrays.equals(start,end);
    }


}
