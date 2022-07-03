package com.whut.database.backend.Tbm;

import com.whut.database.backend.DM.DataManager;
import com.whut.database.backend.VM.VersionManager;
import com.whut.database.backend.parser.statement.*;
import com.whut.database.backend.utils.Parser;
import com.whut.database.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TableManagerImpl implements TableManager {

    VersionManager vm;
    DataManager dm;

    private Booter booter;
    private Map<String,Table> tableCache; // 数据库中所有的表记录
    private Map<Long, List<Table>> xidTableCache; // 某一事务创建的表记录
    private Lock lock;

    public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        this.lock = new ReentrantLock();
        loadTables();
    }

    /*
        加载数据库中的表
     */
    private void loadTables(){
        long uid = firstTableUid();
        while (uid != 0){
            Table table = Table.loadTable(this, uid);
            uid = table.nextUid;
            tableCache.put(table.name,table);
        }
    }

    /*
        获取第一个表的uid
     */
    private long firstTableUid(){
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    /*
        更新第一个表的uid
     */
    private void updateFirstTableUid(long uid){
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    /*
        提交
     */
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    /*
        放弃
     */
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    /*
        获取数据库中表的信息
     */
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table table : tableCache.values()) {
                sb.append(table.toString()).append("\n");
            }
            List<Table> tables = xidTableCache.get(xid);
            if (tables == null) return "\n".getBytes();
            for (Table table : tables) {
                sb.append(table.toString()).append("\n");
            }
            return sb.toString().getBytes();
        }finally {
            lock.unlock();
        }
    }

    /*
        新建表
     */
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            // 表已经存在
            if (tableCache.containsKey(create.tableName)) throw Error.DuplicatedTableException;

            // 创建新表
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);

            tableCache.put(create.tableName,table);
            if (!xidTableCache.containsKey(xid)){
                xidTableCache.put(xid,new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);

            return ("create " + create.tableName).getBytes();
        }finally {
            lock.unlock();
        }
    }

    /*
        向表中插入记录
     */
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();

        if (table == null) throw Error.TableNotFoundException;

        table.insert(xid,insert);
        return "insert".getBytes();
    }

    /*
        读取记录
     */
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();

        if (table == null) throw Error.TableNotFoundException;

        return table.read(xid, read).getBytes();
    }

    /*
        更新记录
     */
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();

        if (table == null) throw Error.TableNotFoundException;

        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    /*
        删除记录
     */
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();

        if (table == null) throw Error.TableNotFoundException;

        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }
}
