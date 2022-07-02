package com.whut.database.backend.parser.statement;

/*
    只支持全字段插入
 */
public class Insert {
    public String tableName;
    public String[] values;
}
