package com.whut.database.backend.Tbm;

import com.google.common.primitives.Bytes;
import com.sun.javafx.collections.MappingChange;
import com.whut.database.backend.TM.TransactionManagerImpl;
import com.whut.database.backend.Tbm.Field.ParseValueRes;
import com.whut.database.backend.parser.statement.*;
import com.whut.database.backend.utils.Panic;
import com.whut.database.backend.utils.ParseStringRes;
import com.whut.database.backend.utils.Parser;
import com.whut.database.common.Error;
import org.checkerframework.checker.units.qual.C;

import java.util.*;

/*
    表结构
        [TableName][NextTable]
        [Field1Uid][Field2Uid]...[FieldNUid]
        TableName：表名    NextTable：下一张表     FieldNUid：表中的字段

        链表的方式存储数据库中的表
 */
public class Table {

    TableManager tbm;
    long uid;
    String name;
    long nextUid;
    List<Field> fields = new ArrayList<>();

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /*
        读取表
     */
    public static Table loadTable(TableManager tbm, long uid){
        byte[] raw = null;
        try{
            raw = ((TableManagerImpl) tbm).vm.read(TransactionManagerImpl.SUPER_XID,uid);
        } catch (Exception e) {
            Panic.panic(e);
        }

        assert raw != null;
        Table tb = new Table(tbm,uid);

        return tb.parseSelf(raw);
    }

    /*
        解析表结构
     */
    private Table parseSelf(byte[] raw){
        int pos = 0;

        // 解析表名
        ParseStringRes res = Parser.parseString(raw);
        this.name = res.str;

        // 解析下一个表
        pos += res.next;
        this.nextUid = Parser.parseLong(Arrays.copyOfRange(raw,pos,pos+8));

        // 解析字段
        pos += 8;
        while(pos < raw.length){
            long uid = Parser.parseLong(Arrays.copyOfRange(raw,pos,pos+8));
            pos += 8;
            fields.add(Field.loadField(this,uid));
        }

        return this;
    }

    /*
        创建表
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception{
        Table tb = new Table(tbm,create.tableName,nextUid);
        for(int i = 0; i < create.filedName.length; i++){
            String fileName = create.filedName[i];
            String fileType = create.fieldType[i];
            boolean index = false;
            for(int j = 0; j < create.index.length; j++){
                if (fileName.equals(create.index[j])){
                    index = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb,xid,fileName,fileType,index));
        }

        return tb.persistSelf(xid);
    }

    /*
        将表结构转成Entry进行插入
     */
    private Table persistSelf(long xid) throws Exception{
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw,Parser.long2Byte(field.uid));
        }
        this.uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));

        return this;
    }

    /*
        往表中插入数据
     */
    public void insert(long xid, Insert insert) throws Exception{
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl)tbm).vm.insert(xid,raw);
        for (Field field : fields) {
            if (field.isIndexed()){ // 是索引字段，在自己的B+树上插入节点
                field.insert(entry.get(field.fieldName),uid);
            }
        }
    }

    private Map<String, Object> string2Entry(String[] values) throws Exception{
        // 只支持全字段插入
        if (values.length != fields.size()){
            throw Error.InvalidCommandException;
        }

        Map<String,Object> entry = new HashMap<>();
        for(int i = 0; i < fields.size(); i++){
            Field field = fields.get(i);
            Object value = field.string2Value(values[i]);
            entry.put(field.fieldName,value);
        }

        return entry;
    }

    /*
        将一条数据记录转化成raw
     */
    private byte[] entry2Raw(Map<String, Object> entry){
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw,field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    /*
        删除表中的记录
     */
    public int delete(long xid, Delete delete) throws Exception{
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid,uid)){
                count++;
            }
        }

        return count;
    }

    /*
        更新表中的记录
     */
    public int update(long xid, Update update) throws Exception{
        List<Long> uids = parseWhere(update.where);

        // 找到表中需要更新的字段
        Field field = null;
        for (Field fd : fields) {
            if (fd.fieldName.equals(update.fieldName)){
                field = fd;
                break;
            }
        }
        if (field == null) throw Error.FieldNotFoundException;

        Object value = field.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;

            // 先删除表中的记录
            ((TableManagerImpl) tbm).vm.delete(xid,uid);

            // 再插入新的记录
            Map<String,Object> entry = parseEntry(raw);
            entry.put(field.fieldName,value);
            raw = entry2Raw(entry);
            long uuid = ((TableManagerImpl) tbm).vm.insert(xid,raw);

            // 判断新的属性是否是索引，是的话，重新加入B+树
            /*
                question：原来的索引记录没有从B+树中删除
             */
            for (Field fd : fields) {
                if (fd.isIndexed()){
                    fd.insert(entry.get(fd.fieldName),uuid);
                }
            }

            count++;
        }

        return count;
    }

    /*
        从表中读取数据
     */
    public String read(long xid, Select select) throws Exception{
        List<Long> uids = parseWhere(select.where);
        StringBuilder sb = new StringBuilder();

        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;

            Map<String,Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }

        return sb.toString();
    }

    /*
        获取列名和列值
     */
    private Map<String,Object> parseEntry(byte[] raw){
        int pos = 0;
        Map<String,Object> entry = new HashMap<>();
        for (Field field : fields) {
            ParseValueRes res = field.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName,res.value);
            pos += res.shift;
        }
        return entry;
    }

    /*
        打印一条记录
     */
    private String printEntry(Map<String,Object> entry){
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if (i == fields.size() - 1) sb.append("]");
            else sb.append(",");
        }
        return sb.toString();
    }

    /*
        解析Where条件，选择出符合条件的uids
     */
    private List<Long> parseWhere(Where where) throws Exception{
        long l0 = 0;
        long r0 = 0;
        long l1 = 0;
        long r1 = 0;
        Field field = null;
        boolean single = false;

        if (where == null){
            for (Field fd : fields) {
                if (fd.isIndexed()){
                    field = fd;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        }else{
            for (Field fd : fields) {
                if (fd.fieldName.equals(where.singleExp1.field)){
                    if(!field.isIndexed()){ // where条件的字段必须是索引字段
                        throw Error.FieldNotIndexedException;
                    }
                    field = fd;
                    break;
                }
            }
            if (field == null) throw Error.FieldNotFoundException;

            CalWhereRes res = calWhere(field,where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }

        List<Long> uids = field.search(l0,r0);
        if (!single){
            List<Long> tmp = field.search(l1,r1);
            uids.addAll(tmp);
        }

        return uids;
    }

    class CalWhereRes{
        long l0,r0,l1,r1;
        boolean single;
    }


    private CalWhereRes calWhere(Field fd, Where where) throws Exception{
        CalWhereRes res = new CalWhereRes();
        FieldCalRes fRes = null;
        switch (where.logicOp){
            case "":
                res.single = true;
                fRes = fd.calExp(where.singleExp1);
                res.l0 = fRes.left;
                res.r0 = fRes.right;
                break;
            case "and":
                res.single = true;
                fRes = fd.calExp(where.singleExp1);
                res.l0 = fRes.left;
                res.r0 = fRes.right;
                fRes = fd.calExp(where.singleExp2);
                res.l1 = fRes.left;
                res.r1 = fRes.right;

                // 取条件的交集部分
                if(res.l1 > res.l0) res.l0 = res.l1;
                if (res.r1 < res.r0) res.r0 = res.r1;
                break;
            case "or":
                res.single = false;
                fRes = fd.calExp(where.singleExp1);
                res.l0 = fRes.left;
                res.r0 = fRes.right;
                fRes = fd.calExp(where.singleExp2);
                res.l1 = fRes.left;
                res.r1 = fRes.right;
            default:
                throw Error.InvalidLogOpException;
        }

        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append(name).append("：");
        for(int i = 0; i < fields.size(); i++){
            sb.append(fields.get(i).toString());
            if (i == fields.size()-1) sb.append("}");
            else sb.append(",");
        }
        return sb.toString();
    }

}
