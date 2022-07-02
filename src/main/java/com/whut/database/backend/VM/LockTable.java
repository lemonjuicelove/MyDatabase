package com.whut.database.backend.VM;

import com.whut.database.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
    死锁检测：维护有向图，判断有没有环
    防止两阶段锁出现死锁

 */
public class LockTable {

    private Map<Long, List<Long>> x2u; // 某个XID已经获得的资源的UID列表

    private Map<Long,Long> u2x; // UID被某个XID持有

    private Map<Long,List<Long>> wait; // 正在等待某个UID的XID列表

    private Map<Long, Lock> waitLock; // 正在等待资源的XID的锁

    private Map<Long,Long> waitU;  // XID正在等待的UID

    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /*
        事务执行时，添加边，进行死锁检测
        不用等待返回null，需要等待返回锁对象
        出现死锁时抛出异常
     */
    public Lock add(long xid, long uid) throws Exception{
        lock.lock();
        try{
            if(isInList(x2u,xid,uid)){ // 该资源没有被任何xid获取过
                return null;
            }

            if(!u2x.containsKey(uid)){ // 该资源没有被任何xid持有
                u2x.put(uid,xid);
                putIntoList(x2u,xid,uid);
                return null;
            }

            // 添加边
            waitU.put(xid,uid);
            putIntoList(wait,uid,xid);

            // 检测是否有死锁
            if (hasDeadLock()){
                // 移除边
                waitU.remove(xid);
                removeFromList(wait,uid,xid);
                throw Error.DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid,l);
            return l;
        }finally {
            lock.unlock();
        }
    }

    /*
        事务提交或放弃时，释放锁，并删除边
     */
    public void remove(long xid){
        lock.lock();
        try {
            List<Long> uidList = x2u.get(xid);
            if (uidList != null){
                while (uidList.size() > 0){
                    Long uid = uidList.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        }finally {
            lock.unlock();
        }
    }

    /*
        从等待队列中选择一个xid来占用uid
     */
    private void selectNewXID(Long uid) {
        u2x.remove(uid);
        List<Long> xidList = wait.get(uid);
        if (xidList == null) return;
        assert xidList.size() > 0;

        while(xidList.size() > 0){
            Long xid = xidList.remove(0);
            if(!waitLock.containsKey(xid)) continue;

            // 公平锁的体现：从等待队列中获取
            u2x.put(uid,xid);
            Lock l = waitLock.get(xid);
            l.unlock();
            break;
        }

        if (xidList.size() == 0) wait.remove(uid);
    }

    /*
        从列表中移除等待的资源
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid, long xid) {
        List<Long> xidList = listMap.get(uid);
        if (xidList == null) return;

        for (Long e : xidList) {
            if(e == xid){
                xidList.remove(e);
                break;
            }
        }
        if(xidList.size() == 0) listMap.remove(uid);
    }

    private Map<Long,Integer> xidStamp;
    private int stamp;

    /*
        死锁检测是否有环
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = -1;

        for (Long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) continue;
            stamp++;
            if (dfs(xid)) return true;
        }

        return false;
    }

    private boolean dfs(Long xid) {
        Integer stp = xidStamp.get(xid);
        if (stp != null && stp == stamp) return true;
        if (stp != null && stp < stamp) return false;

        xidStamp.put(xid,stamp);

        Long uid = waitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    /*
        将uid放入xid所获取的资源列表
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long xid, long uid) {
        if (!listMap.containsKey(xid)){
            listMap.put(xid,new ArrayList<>());
        }
        listMap.get(xid).add(0,uid);
    }

    /*
        检测某个uid是否被xid获取
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long xid, long uid) {
        List<Long> uidList = listMap.get(xid);
        if(uidList == null) return false;
        for (long e : uidList) {
            if (e == uid) return true;
        }

        return false;
    }
}
