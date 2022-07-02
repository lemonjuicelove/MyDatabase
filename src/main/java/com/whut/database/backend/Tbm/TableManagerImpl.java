package com.whut.database.backend.Tbm;

import com.whut.database.backend.DM.DataManager;
import com.whut.database.backend.VM.VersionManager;
import com.whut.database.backend.parser.statement.*;

public class TableManagerImpl implements TableManager {

    VersionManager vm;
    DataManager dm;


    @Override
    public byte[] commit(long xid) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] abort(long xid) {
        return new byte[0];
    }

    @Override
    public byte[] show(long xid) {
        return new byte[0];
    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] read(long xid, Select select) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        return new byte[0];
    }
}
