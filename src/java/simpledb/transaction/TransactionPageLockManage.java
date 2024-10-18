package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
    事务-页面-锁的管理，一个事务在一个页面某一时刻只能拥有一个锁（读锁 or 写锁）
    NOTE: 事务-页面，页面不一定在缓冲区（由于驱逐clean page）
*/

public class TransactionPageLockManage {
    private final Map<TransactionId, Map<PageId, Lock>> tpMap = new HashMap<>();
    private final Map<PageId, Map<TransactionId, Lock>> ptMap = new HashMap<>();


    public synchronized void addTPLock(TransactionId tid, PageId pageId, Lock lock) {
        tpMap.putIfAbsent(tid, new HashMap<>());
        tpMap.get(tid).put(pageId, lock);

        ptMap.putIfAbsent(pageId, new HashMap<>());
        ptMap.get(pageId).put(tid, lock);
    }

    // 移除事务在该页面上的锁
    public synchronized void removeTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, Lock> nodeMap = tpMap.get(tid);
        if(nodeMap != null) {
            Lock lock = nodeMap.get(pageId);
            lock.unlock();
            nodeMap.remove(pageId);
        }
        Map<TransactionId, Lock> map = ptMap.get(pageId);
        if(map != null) map.remove(tid);
    }

    // 移除事务在所有页面上的锁
    public synchronized void removeAllTPLock(TransactionId tid) {
        Map<PageId, Lock> nodeMap = tpMap.get(tid);
        if(nodeMap == null) return;
        for (Lock lock : nodeMap.values()) {
            lock.unlock();
        }
        tpMap.remove(tid);
        for (Map<TransactionId, Lock> value : ptMap.values()) {
            value.remove(tid);
        }
    }

    // 返回事务在该页面上持有的锁
    public synchronized Lock holdTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, Lock> nodeMap = tpMap.get(tid);
        if(nodeMap == null) return null;
        return nodeMap.get(pageId);
    }

    // 返回所有锁定该页面的事务和锁
    public synchronized Map<TransactionId, Lock> getAllTransactionAndLock(PageId pageId) {
        return ptMap.get(pageId);
    }

}
