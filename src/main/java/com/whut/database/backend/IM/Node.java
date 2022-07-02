package com.whut.database.backend.IM;

import com.whut.database.backend.DM.dataItem.DataItem;
import com.whut.database.backend.TM.TransactionManagerImpl;
import com.whut.database.backend.common.SubArray;
import com.whut.database.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
    二叉树节点：
        [LeafFlag][KeyNumber][SiblingUid]
        [Son0][Key0][Son1][Key1]...[SonN][KeyN]
        LeafFlag：是否是叶子节点    KeyNumber：该节点中key的个数    SiblingUid：兄弟节点的uid
        Key0：key值   KeyN：最后一个key值始终是最大值
 */
public class Node {

    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    /*
        设置叶子节点的标识
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf){
        if (isLeaf){
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        }else{
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    /*
        判断是否是叶子节点
     */
    static boolean getRawIsLeaf(SubArray raw){
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    /*
        设置当前节点key的个数
     */
    static void setRawNoKeys(SubArray raw, int noKeys){
        System.arraycopy(Parser.short2Byte((short)noKeys),0,raw.raw,raw.start + NO_KEYS_OFFSET,SIBLING_OFFSET-NO_KEYS_OFFSET);
    }

    /*
        获取当前节点key的个数
     */
    static int getRawNoKeys(SubArray raw){
        return Parser.parseShort(Arrays.copyOfRange(raw.raw,raw.start+NO_KEYS_OFFSET,raw.start+SIBLING_OFFSET));
    }

    /*
        设置兄弟节点的uid
     */
    static void setRawSibling(SubArray raw, long sibling){
        System.arraycopy(Parser.long2Byte(sibling),0,raw.raw,raw.start + SIBLING_OFFSET,raw.start + NODE_HEADER_SIZE - SIBLING_OFFSET);
    }

    /*
        获取兄弟节点的uid
     */
    static long getRawSibling(SubArray raw){
        return Parser.parseLong(Arrays.copyOfRange(raw.raw,raw.start + SIBLING_OFFSET,raw.start+NODE_HEADER_SIZE));
    }

    /*
        设置第k个子节点的uid
     */
    static void setRawKthSon(SubArray raw, long uid, int kth){
        int offset = raw.start + NODE_HEADER_SIZE +kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid),0,raw.raw,offset,8);
    }

    /*
        获取第k个子节点的uid
     */
    static long getRawKthSon(SubArray raw, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw,offset,offset+8));
    }

    /*
        设置第k个子节点的key
     */
    static void setRawKthKey(SubArray raw, long key, int kth){
        int offset = raw.start + NODE_HEADER_SIZE +kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key),0,raw.raw,offset,8);
    }

    /*
        获取第k个子节点的key
     */
    static long getRawKthKey(SubArray raw, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth*(8*2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw,offset,offset+8));
    }

    /*
        子节点分裂的时候，进行拷贝
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth){
        int offset = from.start + NODE_HEADER_SIZE + kth*(8*2);
        System.arraycopy(from.raw,offset,to.raw,to.start+NODE_HEADER_SIZE,from.end-offset);
    }

    /*
        从第k个子节点开始移位
     */
    static void shiftRawKth(SubArray raw, int kth){
        int begin = raw.start +NODE_HEADER_SIZE + (kth+1)*(8*2);
        int end = raw.start + NODE_SIZE - 1;
        for(int i = end; i >= begin; i--){
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    /*
        生成一个新的根节点
     */
    static byte[] newRootRaw(long left, long right, long key){
        SubArray raw = new SubArray(new byte[NODE_SIZE],0,NODE_SIZE);

        setRawIsLeaf(raw,false); // 非叶子节点
        setRawNoKeys(raw,2); // 两个叶子节点
        setRawSibling(raw,0); // 没有兄弟节点
        setRawKthSon(raw,left,0); // 第一个子节点
        setRawKthKey(raw,key,0); // 第一个子节点的key
        setRawKthSon(raw,right,1); // 第二个子节点
        setRawKthKey(raw,Long.MAX_VALUE,1); // 最后一个子节点的key默认是最大值

        return raw.raw;
    }

    /*
        生成一个空的根节点
     */
    static byte[] newNilRootRaw(){
        SubArray raw = new SubArray(new byte[NODE_SIZE],0,NODE_SIZE);

        setRawIsLeaf(raw,true); // 叶子节点
        setRawNoKeys(raw,0); // 没有叶子节点
        setRawSibling(raw,0); // 没有兄弟节点

        return raw.raw;
    }

    /*
        从B+树索引中获取数据
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception{
        DataItem di = bTree.dm.read(uid);
        assert di != null;

        Node node = new Node();
        node.tree = bTree;
        node.dataItem = di;
        node.raw = di.data();
        node.uid = uid;

        return node;
    }

    public void release(){
        dataItem.release();
    }

    /*
        判断是否是叶子节点
     */
    public boolean isLeaf(){
        dataItem.rLock();
        try {
            return getRawIsLeaf(raw);
        }finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes {
        long uid; // 自己的uid
        long siblingUid; // 兄弟节点的uid
    }

    /*
        根据key找到对应的节点，如果没有，返回兄弟节点的uid
     */
    public SearchNextRes searchNext(long key){
        dataItem.rLock();
        try{
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw); // 获取当前节点的key的数量
            for(int i = 0; i < noKeys; i++){
                long ik = getRawKthKey(raw,i);
                // 要找的数据在当前节点的树上
                if (key < ik){
                    res.uid = getRawKthSon(raw,i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            // 要找的key不在当前节点的树上
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        }finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids; // 当前节点存储的子节点的uid
        long siblingUid; // 兄弟节点的uid
    }

    /*
        对当前节点的子节点进行范围查询
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey){
        dataItem.rLock();
        try{
            int noKeys = getRawNoKeys(raw);
            int kth = 0;

            // 获取区间范围内的节点
            while (kth < noKeys){
                long ik = getRawKthKey(raw,kth);
                if(ik >= leftKey) break;;
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys){
                long ik = getRawKthKey(raw,kth);
                if (ik > rightKey) break;
                uids.add(getRawKthSon(raw,kth));
                kth++;
            }

            // 如果右区间没有结束，将兄弟节点uid也返回
            long siblingUid = 0;
            if (kth == noKeys) siblingUid = getRawSibling(raw);
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;

            return res;
        }finally {
            dataItem.rUnLock();
        }
    }

    /*
        当前节点是否需要分裂
     */
    private boolean needSplit(){
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes{
        long newSon;
        long newKey;
    }

    /*
        节点分裂后，返回新分裂出来的节点的第一个子节点
     */
    private SplitRes split() throws Exception{
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE],0,NODE_SIZE);
        setRawIsLeaf(nodeRaw,getRawIsLeaf(raw));
        setRawNoKeys(nodeRaw,BALANCE_NUMBER);
        setRawSibling(nodeRaw,getRawSibling(raw));
        copyRawFromKth(raw,nodeRaw,BALANCE_NUMBER); // 拷贝一半的数据到分裂的节点上
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID,nodeRaw.raw);

        // 更新被分裂的节点
        setRawNoKeys(raw,BALANCE_NUMBER);
        setRawSibling(raw,son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw,0);
        return res;
    }

    class InsertAndSplitRes {
        long siblingUid;
        long newSon;
        long newKey;
    }

    public InsertAndSplitRes insertAndSplitRes(long uid, long key) throws Exception{
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try{

            success = insert(uid,key);
            if (!success){ // 插入失败
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            // 插入成功，判断是否需要分裂
            if (needSplit()){
                try{
                    SplitRes split = split();
                    res.newSon = split.newSon;
                    res.newKey = split.newKey;
                    return res;
                }catch (Exception e){
                    err = e;
                    throw err;
                }
            }else{
                return res;
            }

        }finally {
            if(err == null && success) dataItem.after(TransactionManagerImpl.SUPER_XID);
            else dataItem.unBefore();
        }
    }

    /*
        在当前位置插入子节点
     */
    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;

        while(kth < noKeys){
            long ik = getRawKthKey(raw,kth);
            if (ik >= key) break;
            kth++;
        }

        // 当前位置不能再插入节点
        if (kth == noKeys && getRawSibling(raw) != 0) return false;

        shiftRawKth(raw,kth);
        setRawKthSon(raw,uid,kth);
        setRawKthKey(raw,key,kth);
        setRawNoKeys(raw,noKeys+1);

        return true;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIsLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
