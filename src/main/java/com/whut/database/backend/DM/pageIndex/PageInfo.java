package com.whut.database.backend.DM.pageIndex;

/*
    页信息
 */
public class PageInfo {

    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }

}
