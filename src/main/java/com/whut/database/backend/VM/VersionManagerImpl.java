package com.whut.database.backend.VM;

import com.whut.database.backend.DM.DataManager;
import com.whut.database.backend.TM.TransactionManager;
import com.whut.database.backend.common.AbstractCache;
import com.whut.database.backend.utils.Panic;
import com.whut.database.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm; // 事务管理器
    DataManager dm; // 数据管理器
    Map<Long,Transaction> activeTransaction; // 活跃事务
    Lock lock; // 锁
    LockTable lt; // 死锁检测器

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        // 将超级事务添加到活跃事务中
        activeTransaction.put(TransactionManager.SUPER_XID,Transaction.newTransaction(TransactionManager.SUPER_XID,0,null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    /*
        读一个Entry
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.err != null) throw t.err;

        Entry entry = null;
        try {
            entry = super.get(uid);
            // 判断该记录对事务可见
            if(Visibility.isVisible(tm,t,entry)){
                return entry.data();
            }else{
                return null;
            }
        }catch (Exception e){
            if (e == Error.NullEntryException) return null;
            else throw e;
        }finally {
            entry.release();
        }

    }

    /*
        插入一个Entry
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.err != null) throw t.err;

        byte[] raw = Entry.wrapEntryRaw(xid, data);

        return dm.insert(xid, raw);
    }

    /*
        删除一个Entry
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) throw t.err;

        Entry entry = null;
        try {
            entry = super.get(xid);

            // 判断可见性
            if (!Visibility.isVisible(tm,t,entry)){
                return false;
            }

            // 获取资源的锁
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            }catch (Exception e){
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            // 只有当xid对应的事务获取到资源之后，才能继续向下执行
            if (l != null){
                l.lock();
                l.unlock();
            }

            if (entry.getXmax() == xid) return false;

            // 判断是否存在版本跳跃问题
            if (Visibility.isVersionSkip(tm,t,entry)){
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;

        }catch (Exception e){
            if (e == Error.NullEntryException) return false;
            else throw e;
        }finally {
            entry.release();
        }

    }

    /*
        开启事务
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid,level,activeTransaction);
            activeTransaction.put(xid,t);
            return xid;
        }finally {
            lock.unlock();
        }
    }

    /*
        提交事务
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if (t.err != null) throw t.err;
        }catch (NullPointerException e){
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(e);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    /*
        手动回滚
     */
    @Override
    public void abort(long xid) {
        internAbort(xid,false);
    }

    /*
        自动回滚
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        // 手动回滚
        if(!autoAborted) activeTransaction.remove(xid);
        lock.unlock();

        if (t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this,uid);
        if (entry == null){
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.release();
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

}
