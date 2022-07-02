package com.whut.database.backend.utils;

/*
    直接退出程序
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
