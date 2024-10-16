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

    // 移除事务在该页面上的所有锁，尽量先释放锁
    public synchronized void removeTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, List<Lock>> nodeMap = tpMap.get(tid);
        if(nodeMap != null) {
            List<Lock> locks = nodeMap.get(pageId);
            for (Lock lock : locks) {
                try {
                    lock.unlock();
                } catch (IllegalMonitorStateException e) {
                    System.out.println("release lock error: " + e.getMessage());
                }
            }
            nodeMap.remove(pageId);
        }
        Map<TransactionId, List<Lock>> map = ptMap.get(pageId);
        if(map != null) map.remove(tid);
    }

    // 移除事务在所有页面上的所有锁，尽量先释放锁
    public synchronized void removeAllTPLock(TransactionId tid) {
        Map<PageId, List<Lock>> nodeMap = tpMap.get(tid);
        if(nodeMap == null) return;
        for (List<Lock> locks : nodeMap.values()) {
            for (Lock lock : locks) {
                try {
                    lock.unlock();
                } catch (IllegalMonitorStateException e) {
                    System.out.println("release lock error: " + e.getMessage());
                }
            }
        }
        tpMap.remove(tid);
        for (Map<TransactionId, List<Lock>> value : ptMap.values()) {
            value.remove(tid);
        }
    }

    // 移除页面上的所有事务和锁，尽量先释放锁
    public synchronized void discardPageAllTPLock(PageId pageId) {
        Map<TransactionId, List<Lock>> map = ptMap.get(pageId);
        if(map == null) return;
        for (List<Lock> locks : map.values()) {
            for (Lock lock : locks) {
                try {
                    lock.unlock();
                } catch (IllegalMonitorStateException e) {
                    System.out.println("release lock error: " + e.getMessage());
                }
            }
        }
        ptMap.remove(pageId);
        for (Map.Entry<TransactionId, Map<PageId, List<Lock>>> entry : tpMap.entrySet()) {
            entry.getValue().remove(pageId);
        }
    }

    // 返回事务在该页面上持有的所有锁
    public synchronized List<Lock> holdTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, List<Lock>> nodeMap = tpMap.get(tid);
        if(nodeMap == null) return null;
        return nodeMap.get(pageId);
    }

    // 返回所有锁定该页面的事务和锁
    public synchronized Map<TransactionId, List<Lock>> getAllTransactionAndLock(PageId pageId) {
        return ptMap.get(pageId);
    }


    // 释放事务在某页面的锁（若当前线程没持有锁，抛出IllegalMonitorStateException），若成功，移除它。
    // 在HeapFile.insertTuple时使用。
    public synchronized void weakRemoveTPLock(TransactionId tid, PageId pageId) throws IllegalMonitorStateException {
        Map<PageId, List<Lock>> nodeMap = tpMap.get(tid);
        if(nodeMap != null) {
            List<Lock> locks = nodeMap.get(pageId);
            for (Lock lock : locks) {
                lock.unlock(); // may fail
            }
            nodeMap.remove(pageId);
        }
        Map<TransactionId, List<Lock>> map = ptMap.get(pageId);
        if(map != null) map.remove(tid);
    }
}
