package com.whut.database.backend.VM;

import com.whut.database.backend.TM.TransactionManager;

/*
    事务的可见性逻辑
 */
public class Visibility {

    /*
        版本跳跃问题：如果 Ti 需要修改 X，而 X 已经被 Ti 不可见的事务 Tj 修改了，那么要求 Ti 回滚
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e){
        long xMax = e.getXmax();
        if(t.level == 0){
            return false;
        }else {
            /*
                tm.isCommitted(xMax)：高版本已经提交
                xMax > t.xid || t.isInSnapshot(xMax)：由高版本的事务或是处在活跃状态的事务
             */
            return tm.isCommitted(xMax) && (xMax > t.xid || t.isInSnapshot(xMax));
        }
    }

    /*
        判断数据是否对该事务可见
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e){
        if(t.level == 0){ // 读已提交
            return readCommitted(tm,t,e);
        }else{ // 可重复读
            return repeatableRead(tm,t,e);
        }
    }

    /*
        读已提交下是否可见
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xMin = e.getXmin();
        long xMax = e.getXmax();
        // 该数据由该事务创建且没有被删除
        if(xMin == xid && xMax == 0) return true;

        if(tm.isCommitted(xMin)){ // 由已提交事务创建
            if (xMax == 0) return true; // 没有被删除
            // 由未提交事务删除
            if(xMax != xid && !tm.isCommitted(xMax)) return true;
        }

        return false;
    }

    /*
        可重复读下是否可见
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xMin = e.getXmin();
        long xMax = e.getXmax();
        // 该数据由该事务创建且没有被删除
        if(xMin == xid && xMax == 0) return true;

        /*
            tm.isCommitted(xMin) && xMin < xid：由低版本事务提交
            !t.isInSnapshot(xMin)：在当前事务开始时，低版本事务已经完成
         */
        if (tm.isCommitted(xMin) && xMin < xid && !t.isInSnapshot(xMin)){
            if (xMax == 0) return true;
            if (xMax != xid){
                /*
                    !tm.isCommitted(xMax)：由未提交事务删除
                    xMax > xid：由高版本事务执行的
                    t.isInSnapshot(xMax)：在当前事务开始时，处在活跃状态的事务
                 */
                if(!tm.isCommitted(xMax) || xMax > xid || t.isInSnapshot(xMax)) return true;
            }
        }

        return false;
    }


}
