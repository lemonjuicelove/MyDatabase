package com.whut.database.backend.parser.statement;

/*
    只支持单字段更新
 */
public class Update {
    public String tableName;
    public String fieldName;
    public String value;
    public Where where;
}
