package com.whut.database.backend.Tbm;

import com.google.common.primitives.Bytes;
import com.whut.database.backend.IM.BPlusTree;
import com.whut.database.backend.TM.TransactionManagerImpl;
import com.whut.database.backend.parser.statement.SingleExpression;
import com.whut.database.backend.utils.Panic;
import com.whut.database.backend.utils.ParseStringRes;
import com.whut.database.backend.utils.Parser;
import com.whut.database.common.Error;

import java.util.Arrays;
import java.util.List;

/*
    字段结构
        [FieldName][TypeName][IndexUid]
        FieldName：字段名    TypeName：类型名    IndexUid：该字段不是索引的话，1为0
 */
public class Field {

    long uid;
    String fieldName;
    String fieldType;

    private long index;
    private Table table;
    private BPlusTree tree;

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.table = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.table = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /*
        从表中加载字段
     */
    public static Field loadField(Table tb, long uid){
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID,uid);
        }catch (Exception e){
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid,tb).parseSelf(raw);
    }

    /*
        将raw封装为Field字段
     */
    private Field parseSelf(byte[] raw){
        int pos = 0;
        ParseStringRes res = Parser.parseString(raw);
        this.fieldName = res.str;
        pos += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw,pos,raw.length));
        this.fieldType = res.str;
        pos += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw,pos,raw.length));

        if (this.index != 0){ // 说明该字段是索引字段
            try{
                // 获取该索引字段的B+树
                tree = BPlusTree.load(index,((TableManagerImpl)table.tbm).dm);
            }catch (Exception e){
                Panic.panic(e);
            }
        }
        return this;
    }

    /*
        创建字段
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean isIndex) throws Exception{
        typeCheck(fieldType);

        Field field = new Field(tb,fieldName,fieldType,0);
        if (isIndex){ // 该字段是索引
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index,((TableManagerImpl)tb.tbm).dm);
            field.index = index;
            field.tree = bt;
        }

        field.persistSelf(xid);
        return field;
    }

    /*
        检查字段类型是否合法
     */
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    /*
        将该字段写入底层
     */
    private void persistSelf(long xid) throws Exception{
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);

        byte[] fieldRaw = Bytes.concat(nameRaw,typeRaw,indexRaw);
        this.uid = ((TableManagerImpl)table.tbm).vm.insert(xid,fieldRaw);
    }

    /*
        判断该字段是否是索引字段
     */
    public boolean isIndexed(){
        return index != 0;
    }

    /*
        插入一个值
     */
    public void insert(Object key, long uid) throws Exception{
        long uKey = value2Uid(key);
        tree.insert(uKey,uid);
    }

    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                uid = (long)key;
                break;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    /*
        将SQL语句中的值转化成真正的类型
     */
    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }

    /*
        范围查询
     */
    public List<Long> search(long left, long right) throws Exception{
        return tree.searchRange(left,right);
    }

    class ParseValueRes{
        Object v;
        int shift;
    }

    public ParseValueRes parseValue(byte[] raw){
        ParseValueRes res = new ParseValueRes();
        switch (fieldType){
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw,4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseInt(Arrays.copyOf(raw,8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
        }
        return res;
    }

    /*
        输出该字段的值
     */
    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    /*
        解析where条件范围
     */
    public FieldCalRes calExp(SingleExpression exp) throws Exception{
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.operation){
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                // 小于，所以-1
                if (res.right > 0) res.right--;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                // 大于，所以+1
                if (res.left > 0) res.left++;
                break;
            case "=":
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                res.left = res.right;
                break;
        }
        return res;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }



}
