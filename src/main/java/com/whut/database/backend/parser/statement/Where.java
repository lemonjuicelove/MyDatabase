package com.whut.database.backend.parser.statement;

/*
    最多支持两个条件，只支持and或or
 */
public class Where {
    public SingleExpression singleExp1;
    public String logicOp; // 逻辑符
    public SingleExpression singleExp2;
}
