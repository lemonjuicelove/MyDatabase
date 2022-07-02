package com.whut.database.backend.utils;

/*
    将页号和偏移量转化为uid
 */
public class Types {

    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }

}
