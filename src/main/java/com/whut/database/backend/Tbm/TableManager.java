package com.whut.database.backend.Tbm;

import com.whut.database.backend.DM.DataManager;
import com.whut.database.backend.VM.VersionManager;
import com.whut.database.backend.parser.statement.*;
import com.whut.database.backend.utils.Parser;

/*
    表的管理
 */
public interface TableManager {

    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    BeginRes begin(Begin begin);

    /*
        创建TableManager
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm){
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm,dm,booter);
    }

    /*
        打开TableManager
     */
    public static TableManager open(String path, VersionManager vm, DataManager dm){
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm,dm,booter);
    }

}
