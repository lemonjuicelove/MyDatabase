package com.whut.database.backend.server;

import com.whut.database.backend.Tbm.BeginRes;
import com.whut.database.backend.Tbm.TableManager;
import com.whut.database.backend.parser.Parser;
import com.whut.database.backend.parser.statement.*;
import com.whut.database.common.Error;

public class Executor {

    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.xid = 0;
        this.tbm = tbm;
    }

    public void close(){
        if (xid != 0){
            System.out.println("Abnormal Abort：" + xid);
            tbm.abort(xid);
        }
    }

    /*
        根据不同的SQL语句对象调用不同的方法处理
     */
    public byte[] execute(byte[] sql) throws Exception{
        System.out.println("Execute：" + new String(sql));
        Object statement = Parser.Parse(sql);
        if (statement instanceof Begin){
            if (xid != 0) throw Error.NestedTransactionException;
            BeginRes res = tbm.begin((Begin) statement);
            xid = res.xid;
            return res.result;
        }else if (statement instanceof Commit){
            if (xid == 0) throw Error.NoTransactionException;
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        }else if (statement instanceof Abort){
            if (xid == 0) throw Error.NoTransactionException;
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        }else {
            return execute2(statement);
        }
    }

    private byte[] execute2(Object statement) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if (xid == 0){
            tmpTransaction = true;
            BeginRes res = tbm.begin(new Begin());
            xid = res.xid;
        }

        try{
            byte[] res = null;
            if(statement instanceof Show){
                res = tbm.show(xid);
            }else if (statement instanceof Create){
                res = tbm.create(xid,(Create) statement);
            }else if (statement instanceof Select){
                res = tbm.read(xid,(Select) statement);
            }else if (statement instanceof Insert){
                res = tbm.insert(xid,(Insert) statement);
            }else if (statement instanceof Delete){
                res = tbm.delete(xid,(Delete) statement);
            }else if (statement instanceof Update){
                res = tbm.update(xid,(Update) statement);
            }
            return res;
        } catch (Exception ex) {
            e = ex;
            throw e;
        }finally {
            if (tmpTransaction){
                if (e != null){
                    tbm.abort(xid);
                }else{
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }

}
