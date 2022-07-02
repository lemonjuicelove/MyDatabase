package com.whut.database.backend.DM.page;

/*
    数据页
 */
public interface Page {

    void lock(); // 加锁

    void unlock(); // 解锁

    void release(); // 释放数据页

    void setDirty(boolean dirty); // 设置脏页

    boolean isDirty(); // 判断是否为脏页

    int getPageNumber(); // 获取页号

    byte[] getData(); // 获取数据

}
