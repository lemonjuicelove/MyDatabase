package com.whut.database.backend.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.whut.database.common.Error;



/**
 * 使用引用计数器的缓存策略
 */
public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> isGetting;             // 该资源是否正在被获取

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;



    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        isGetting = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 获取资源
    protected T get(long key) throws Exception{

        // 死循环获取资源
        while (true){
            lock.lock();

            if(isGetting.containsKey(key)){
                // 当前资源被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                }catch (InterruptedException e){
                }
                continue;
            }

            if(cache.containsKey(key)){
                // 资源在缓存中，直接返回
                T source = cache.get(key);
                references.put(key,references.get(key)+1);
                lock.unlock();
                return source;
            }

            if (maxResource > 0 && count == maxResource){
                lock.unlock();
                throw Error.CacheFullException;
            }

            // 资源不在缓存中
            isGetting.put(key,true);
            lock.unlock();
            break;
        }

        // 获取资源
        T source = null;
        try {
            source = getForCache(key);
        }catch (Exception e){
            // 出现异常，回滚上面的操作
            lock.lock();
            isGetting.remove(key);
            lock.unlock();
            throw e;
        }

        // 将资源放入缓存
        lock.lock();
        count++;
        isGetting.remove(key);
        cache.put(key,source);
        references.put(key,1);
        lock.unlock();

        return source;
    }

    /*
        释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key);
            if(--ref == 0) { // 如果缓存为0，释放缓存
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else { // 将计数器的值-1
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /*
        缓存关闭，将缓存的数据全部写回
     */
    protected void close(){
        lock.lock();
        try{
            for (Map.Entry<Long, T> entry : cache.entrySet()) {
                T source = entry.getValue();
                releaseForCache(source);
            }
            cache.clear();
            references.clear();
            isGetting.clear();
            count = 0;
        }finally {
            lock.unlock();
        }
    }

    /*
        获取不在缓存中的数据
     */
    protected abstract T getForCache(long key) throws Exception;

    /*
        当资源从缓存中移除的操作
     */
    protected abstract void releaseForCache(T source);


}
