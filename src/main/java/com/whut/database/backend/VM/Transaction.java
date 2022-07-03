package com.whut.database.backend.VM;

import com.whut.database.backend.TM.TransactionManager;
import com.whut.database.backend.TM.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/*
    抽象事务
 */
public class Transaction {

    public long xid; // 事务的自增id

    public int level; // 事务的隔离级别：0为读已提交，1为可重复读

    public Map<Long,Boolean> snapshot; // 记录事务开始时正在活跃的事务

    public Exception err;

    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long,Transaction> active){
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0){ // 将活跃事务放入快照中
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()){
                t.snapshot.put(x,true);
            }
        }

        return t;
    }

    /*
        判断事务是否在活跃事务当中
     */
    public boolean isInSnapshot(long xid){
        if (xid == TransactionManager.SUPER_XID) return false; // 超级事务

        return snapshot.containsKey(xid);
    }


}
