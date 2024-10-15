package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.locks.Lock;

public class TransactionPageLockManage {
    // 可同时持有读锁和写锁
    private final Map<TransactionId, Map<PageId, List<Lock>>> tpMap = new HashMap<>();
    private final Map<PageId, Map<TransactionId, List<Lock>>> ptMap = new HashMap<>();


    public synchronized void addTPLock(TransactionId tid, PageId pageId, Lock lock) {
        tpMap.putIfAbsent(tid, new HashMap<>());
        tpMap.get(tid).putIfAbsent(pageId, new ArrayList<>(2));
        tpMap.get(tid).get(pageId).add(lock);

        ptMap.putIfAbsent(pageId, new HashMap<>());
        ptMap.get(pageId).putIfAbsent(tid, new ArrayList<>(2));
        ptMap.get(pageId).get(tid).add(lock);
    }

    public synchronized void removeTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, List<Lock>> nodeMap = tpMap.get(tid);
        if(nodeMap != null) nodeMap.remove(pageId);

        Map<TransactionId, List<Lock>> map = ptMap.get(pageId);
        if(map != null) map.remove(tid);
    }

    public synchronized List<Lock> holdTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, List<Lock>> nodeMap = tpMap.get(tid);
        if(nodeMap == null) return null;
        return nodeMap.get(pageId);
    }

    public synchronized Map<TransactionId, List<Lock>> getAllTransactionAndLock(PageId pageId) {
        return ptMap.get(pageId);
    }

    public synchronized void releaseTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, List<Lock>> nodeMap = tpMap.get(tid);
        if(nodeMap == null) return;
        List<Lock> locks = nodeMap.get(pageId);
        if(locks != null) {
            for (Lock lock : locks) {
                lock.unlock();
            }
        }
    }

    // 直接废弃页面上的所有锁
    public synchronized void discardAllPageLock(PageId pageId) {
        ptMap.remove(pageId);
        for (Map.Entry<TransactionId, Map<PageId, List<Lock>>> entry : tpMap.entrySet()) {
            entry.getValue().remove(pageId);
        }
    }
}
