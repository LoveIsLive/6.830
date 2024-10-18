package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PageRWLockManage {
    private static final Map<PageId, ReadWriteLock> map = new HashMap<>();

    public synchronized static ReadWriteLock getRWLock(PageId pageId) {
        map.putIfAbsent(pageId, new ReentrantReadWriteLock());
        return map.get(pageId);
    }

    // 用于升级锁
    public synchronized static ReadWriteLock updateRWLock(PageId pageId) {
        ReadWriteLock newLock = new ReentrantReadWriteLock();
        map.put(pageId, newLock);
        return newLock;
    }
}
