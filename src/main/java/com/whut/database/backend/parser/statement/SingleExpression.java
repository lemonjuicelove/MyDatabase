package com.whut.database.backend.parser.statement;

/*
    条件表达式，只支持 > < =
 */
public class SingleExpression {

    public String field; // 字段
    public String operation; // 比较符
    public String value; // 值
}
