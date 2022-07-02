package com.whut.database.backend.IM;

import com.whut.database.backend.DM.DataManager;
import com.whut.database.backend.DM.dataItem.DataItem;
import com.whut.database.backend.IM.Node.InsertAndSplitRes;
import com.whut.database.backend.IM.Node.LeafSearchRangeRes;
import com.whut.database.backend.IM.Node.SearchNextRes;
import com.whut.database.backend.TM.TransactionManagerImpl;
import com.whut.database.backend.common.SubArray;
import com.whut.database.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
    B+树索引
 */
public class BPlusTree {

    DataManager dm;
    long bootUid; // 根节点
    DataItem bootDataItem;
    Lock bootLock;

    /*
        创建一个B+树
     */
    public static long create(DataManager dm) throws Exception{
        byte[] rootRaw = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID,rootRaw);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    /*
        获取B+树
     */
    public static BPlusTree load(long bootUid,DataManager dm) throws Exception{
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;

        BPlusTree tree = new BPlusTree();
        tree.dm = dm;
        tree.bootUid = bootUid;
        tree.bootDataItem = bootDataItem;
        tree.bootLock = new ReentrantLock();

        return tree;
    }

    /*

     */
    private long getRootUid(){
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start,sa.start + 8));
        }finally {
            bootLock.unlock();
        }
    }

    /*
        更新根节点
     */
    private void updateRootUid(long left,long right,long rightKey) throws Exception{
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid),0,diRaw.raw,diRaw.start,8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        }finally {
            bootLock.unlock();
        }
    }

    /*
        根据key寻找对应的叶子节点
     */
    private long searchLeaf(long nodeUid, long key) throws Exception{
        Node node = Node.loadNode(this,nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf){ // 是叶子节点
            return nodeUid;
        }else{ // 不是叶子节点
            long next = searchNext(nodeUid,key);
            return searchLeaf(next,key);
        }
    }

    /*
        根据key寻找对应的节点，当前树节点没有会去兄弟节点接着找
     */
    private long searchNext(long nodeUid,long key) throws Exception{
        while (true){
            Node node = Node.loadNode(this, key);
            SearchNextRes res = node.searchNext(key);
            node.release();

            if (res.uid != 0) return res.uid; // 找到了
            nodeUid = res.siblingUid; // 没找到，去兄弟节点寻找
        }
    }


    public List<Long> search(long key) throws Exception{
        return searchRange(key,key);
    }

    /*
        范围查询
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = getRootUid();
        long leafUid = searchLeaf(rootUid,leftKey);
        List<Long> uids = new ArrayList<>();

        /*
            对叶子节点进行范围查询
         */
        while(true){
            Node leaf = Node.loadNode(this,leafUid);
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();

            uids.addAll(res.uids);
            if (res.siblingUid == 0) break; // 右区间结束
            else leafUid = res.siblingUid; // 右区间没有结束，接着去兄弟节点进行查询
        }

        return uids;
    }


    class InsertRes{
        long newNodeId;
        long newKey;
    }

    /*
        插入一个节点
     */
    public void insert(long key, long uid) throws Exception{
        long rootUid = getRootUid();
        InsertRes res = insert(rootUid,uid,key);

        assert res != null;

        if (res.newNodeId != 0){ // 插入成功并且节点分裂
            // 生成一个新的根节点，原来的根节点和新分裂的节点作为子节点
            updateRootUid(rootUid,res.newNodeId,res.newKey);
        }
    }

    private InsertRes insert(long nodeUid,long uid, long key) throws Exception{
        Node node = Node.loadNode(this,nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if (isLeaf){ // 只有到叶子节点才进行插入操作
            res = insertAndSplit(nodeUid,uid,key);
        }else{
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next,uid,key);
            // 将分裂的节点也插入到叶子节点中
            if (ir.newNodeId != 0) res = insertAndSplit(nodeUid,ir.newNodeId,ir.newKey);
            else res = new InsertRes();
        }

        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception{
        while (true){
            Node node = Node.loadNode(this,nodeUid);
            InsertAndSplitRes ir = node.insertAndSplitRes(uid, key);
            node.release();

            if (ir.siblingUid != 0){ // 该节点插入失败，去兄弟节点插入
                nodeUid = ir.siblingUid;
            }else{ // 该节点能够插入
                InsertRes res = new InsertRes();
                res.newNodeId = ir.newSon;
                res.newKey = ir.newKey;
                return res;
            }
        }
    }

    public void close(){
        bootDataItem.release();
    }


}
