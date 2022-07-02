package com.whut.database.backend.parser;

import com.whut.database.backend.parser.statement.*;
import com.whut.database.common.Error;

import java.util.ArrayList;
import java.util.List;

/*
    解析SQL语句
 */
public class Parser {

    public static Object Parse(byte[] statement) throws Exception{

        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek(); // 获取第一个token
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;

        try{
            switch(token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        }catch (Exception e){
            statErr = e;
        }

        try {
            String next = tokenizer.peek();
            if(!"".equals(next)){
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        }catch (Exception e){
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }

        if (statErr != null) throw statErr;

        return stat;
    }

    /*
        解析show语句
     */
    private static Object parseShow(Tokenizer tokenizer) throws Exception {
        String cur = tokenizer.peek();
        if ("".equals(cur)) return new Show();
        throw Error.InvalidCommandException;
    }

    /*
        解析Update语句：只允许更新单个字段
     */
    private static Object parseUpdate(Tokenizer tokenizer) throws Exception {

        Update update = new Update();

        // 解析表名
        String tableName = tokenizer.peek();
        if(!isName(tableName)) throw Error.InvalidCommandException;
        tokenizer.pop();
        update.tableName = tableName;

        String set = tokenizer.peek();
        if(!"set".equals(set)) throw Error.InvalidCommandException;
        tokenizer.pop();

        // 解析字段名
        String field = tokenizer.peek();
        if(!isName(field)) throw Error.InvalidCommandException;
        tokenizer.pop();
        update.fieldName = field;

        String cur = tokenizer.peek();
        if (!"=".equals(cur)) throw Error.InvalidCommandException;
        tokenizer.pop();

        // 解析字段值
        String value = tokenizer.peek();
        tokenizer.pop();
        update.value = value;

        // 解析where条件
        String where = tokenizer.peek();
        if ("".equals(where)){
            update.where = null;
            return where;
        }

        update.where = parseWhere(tokenizer);
        return update;

    }

    /*
        解析Delete语句
     */
    private static Object parseDelete(Tokenizer tokenizer) throws Exception {

        Delete delete = new Delete();

        String from = tokenizer.peek();
        if(!"from".equals(from)) throw Error.InvalidCommandException;
        tokenizer.pop();

        // 解析表名
        String tableName = tokenizer.peek();
        if(!isName(tableName)) throw Error.InvalidCommandException;
        tokenizer.pop();
        delete.tableName = tableName;

        // 解析where条件
        delete.where = parseWhere(tokenizer);
        return delete;
    }

    /*
        解析Insert语句
     */
    private static Object parseInsert(Tokenizer tokenizer) throws Exception {

        Insert insert = new Insert();

        String into = tokenizer.peek();
        if (!"into".equals(into)) throw Error.InvalidCommandException;
        tokenizer.pop();

        // 解析表名
        String tableName = tokenizer.peek();
        if (!isName(tableName)) throw Error.InvalidCommandException;
        tokenizer.pop();
        insert.tableName = tableName;
        
        String value = tokenizer.peek();
        if(!"value".equals(value)) throw Error.InvalidCommandException;
        
        // 解析属性值
        List<String> values = new ArrayList<>();
        while (true){
            tokenizer.pop();
            String cur = tokenizer.peek();
            if ("".equals(cur)) break;
            values.add(cur);
        }
        insert.values = values.toArray(new String[values.size()]);
        
        return insert;
    }

    /*
        解析Select语句
     */
    private static Object parseSelect(Tokenizer tokenizer) throws Exception {

        // 解析查询字段
        List<String> fields = new ArrayList<>();
        String field = tokenizer.peek();
        if("*".equals(field)){
            fields.add(field);
            tokenizer.pop();
        }else{
            while (true){
                tokenizer.pop();
                String cur = tokenizer.peek();
                if (!isName(cur)) throw Error.InvalidCommandException;
                fields.add(cur);

                tokenizer.pop();
                String next = tokenizer.peek();
                if (",".equals(next)) continue;
                else break;
            }
        }

        Select select = new Select();
        select.fields = fields.toArray(new String[fields.size()]);;
        
        String from = tokenizer.peek();
        if (!"from".equals(from)) throw Error.InvalidCommandException;
        tokenizer.pop();

        // 解析表名
        String tableName = tokenizer.peek();
        if (isName(tableName)) throw Error.InvalidCommandException;
        select.tableName = tableName;
        tokenizer.pop();

        // 解析where条件
        String where = tokenizer.peek();
        if ("".equals(where)) return select;

        select.where = parseWhere(tokenizer);
        return select;

    }

    /*
        解析Where语句
     */
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {

        Where where = new Where();

        String cur = tokenizer.peek();
        if(!"where".equals(cur)) throw Error.InvalidCommandException;
        tokenizer.pop();
        
        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;
        
        String logicOp = tokenizer.peek();
        if("".equals(logicOp)){
            where.logicOp = logicOp;
            return where;
        }
        
        if (!isLogicOp(logicOp)) throw Error.InvalidCommandException;
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;
        
        String end = tokenizer.peek();
        if (!"".equals(end)) throw Error.InvalidCommandException;
        
        return where;
    }

    /*
        解析where条件
     */
    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        
        SingleExpression exp = new SingleExpression();
        String field = tokenizer.peek();
        if (!isName(field)) throw Error.InvalidCommandException;
        tokenizer.pop();
        exp.field = field;
        
        String op = tokenizer.peek();
        if(!isOp(op)) throw Error.InvalidCommandException;
        tokenizer.pop();
        exp.operation = op;
        
        String value = tokenizer.peek();
        exp.value = value;
        tokenizer.pop();
        
        return exp;
    }

    private static boolean isOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    /*
        解析Drop语句
     */
    private static Object parseDrop(Tokenizer tokenizer) throws Exception {

        String table = tokenizer.peek();
        if(!"table".equals(table)) throw Error.InvalidCommandException;

        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) throw Error.InvalidCommandException;

        tokenizer.pop();
        String end = tokenizer.peek();
        if(!"".equals(end)) throw Error.InvalidCommandException;

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    /*
        解析Create语句
     */
    private static Object parseCreate(Tokenizer tokenizer) throws Exception {

        String table = tokenizer.peek();
        if (!"".equals(table)) throw Error.InvalidCommandException;
        tokenizer.pop();

        Create create = new Create();
        String tableName = tokenizer.peek();
        if(!isName(tableName)) throw Error.InvalidCommandException;
        create.tableName = tableName;

        // 解析字段和类型
        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while (true){
            tokenizer.pop();
            String fName = tokenizer.peek();
            if("(".equals(fName)) break;
            if(!isName(fName)) throw Error.InvalidCommandException;
            tokenizer.pop();

            String fType = tokenizer.peek();
            if(!isType(fType)) throw Error.InvalidCommandException;
            tokenizer.pop();

            fNames.add(fName);
            fTypes.add(fType);

            String next = tokenizer.peek();
            if(",".equals(next)){
                continue;
            }else if ("".equals(next)){
                throw Error.TableNoIndexException;
            }else if ("(".equals(next)){
                break;
            }else {
                throw Error.InvalidCommandException;
            }
        }
        create.filedName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        // 解析索引
        tokenizer.pop();
        String index = tokenizer.peek();
        if(!"index".equals(index)) throw Error.InvalidCommandException;

        List<String> idxs = new ArrayList<>();
        while (true){
            tokenizer.pop();
            String idx = tokenizer.peek();
            if(")".equals(idx)) break;
            if (!isName(idx)) throw Error.InvalidCommandException;
            idxs.add(idx);
        }
        create.index = idxs.toArray(new String[idxs.size()]);

        tokenizer.pop();
        String end = tokenizer.peek();
        if(!"".equals(end)) throw Error.InvalidCommandException;

        return create;

    }

    /*
        检查表名是否合理
     */
    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }

    /*
        检查数据类型
     */
    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }

    /*
        解析Abort语句
     */
    private static Object parseAbort(Tokenizer tokenizer) throws Exception {
        String next = tokenizer.peek();
        if (!"".equals(next)) throw Error.InvalidCommandException;

        return new Abort();
    }

    /*
        解析Commit语句
     */
    private static Object parseCommit(Tokenizer tokenizer) throws Exception {
        String next = tokenizer.peek();
        if (!"".equals(next)) throw Error.InvalidCommandException;

        return new Commit();
    }

    /*
        解析begin语句
     */
    private static Object parseBegin(Tokenizer tokenizer) throws Exception{
        String isolation = tokenizer.peek();
        Begin begin = new Begin();

        // 默认读已提交
        if("".equals(isolation)) return begin;

        if (!"isolation".equals(isolation)) throw Error.InvalidCommandException;
        tokenizer.pop();

        String level = tokenizer.peek();
        if (!"level".equals(isolation)) throw Error.InvalidCommandException;
        tokenizer.pop();

        String next1 = tokenizer.peek();
        if ("read".equals(next1)){
            tokenizer.pop();

            String next2 = tokenizer.peek();
            if("committed".equals(next2)){
                tokenizer.pop();

                String end = tokenizer.peek();
                if (!"".equals(end)) throw Error.InvalidCommandException;
            }else {
                throw Error.InvalidCommandException;
            }
        }else if ("repeatable".equals(next1)){
            tokenizer.pop();

            String next2 = tokenizer.peek();
            if("read".equals(next2)){
                begin.isRepeatableRead = true;
                tokenizer.pop();

                String end = tokenizer.peek();
                if (!"".equals(end)) throw Error.InvalidCommandException;
            }else {
                throw Error.InvalidCommandException;
            }
        }else{
            throw Error.InvalidCommandException;
        }

        return begin;
    }
}
