package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

// 使用StampedLock可以解决问题(一个事务多个线程处理的问题),且提供了升级锁的方法.
public class PageRWLockManage {
    private final Map<PageId, StampedLock> map = new HashMap<>();

    public synchronized StampedLock getRWLock(PageId pageId) {
        map.putIfAbsent(pageId, new StampedLock()); // 使用StampedLock
        return map.get(pageId);
    }

}
