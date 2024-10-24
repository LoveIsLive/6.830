package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PageLockManage {
    static class TransactionInfo {
        TransactionId tid;
        Permissions perm;

        public TransactionInfo(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TransactionInfo that = (TransactionInfo) o;
            return Objects.equals(tid, that.tid) && perm == that.perm;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tid, perm);
        }
    }

    static class LockItem {
        ReentrantLock lock; // 页面上的锁
        Map<TransactionId, TransactionInfo> tidInfos; // 持有页面的事务和相应的权限，

        public LockItem(ReentrantLock lock, Map<TransactionId, TransactionInfo> tidInfos) {
            this.lock = lock;
            this.tidInfos = tidInfos;
        }
    }

    private final Map<PageId, LockItem> map = new ConcurrentHashMap<>(); // 并发map

    public boolean accessPage(TransactionId tid, PageId pageId, Permissions perm) {
        map.putIfAbsent(pageId, new LockItem(new ReentrantLock(), new HashMap<>()));
        LockItem lockItem = map.get(pageId); // 保证每个页面只会有一个锁
        assert lockItem != null;
        lockItem.lock.lock();
        try {
            Map<TransactionId, TransactionInfo> tidInfos = lockItem.tidInfos;
            TransactionInfo tidInfo = tidInfos.get(tid);
            if(tidInfo != null) {
                if(perm == Permissions.READ_WRITE && tidInfo.perm == Permissions.READ_ONLY) {
                    if(tidInfos.size() == 1) { // 即仅有自己
                        tidInfo.perm = Permissions.READ_WRITE;
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return true;
                }
            } else {
                if(perm == Permissions.READ_ONLY) {
                    if(tidInfos.size() != 1) { // 说明不可能有写
                        tidInfos.put(tid, new TransactionInfo(tid, perm));
                        return true;
                    } else {
                        TransactionInfo only = tidInfos.values().iterator().next();
                        if(only.perm != Permissions.READ_WRITE) {
                            tidInfos.put(tid, new TransactionInfo(tid, perm));
                            return true;
                        } else {
                            return false;
                        }
                    }
                } else {
                    if(tidInfos.size() == 0) {
                        tidInfos.put(tid, new TransactionInfo(tid, perm));
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        } finally {
            lockItem.lock.unlock();
        }
    }

    public void releasePage(TransactionId tid, PageId pageId) {
        map.putIfAbsent(pageId, new LockItem(new ReentrantLock(), new HashMap<>()));
        LockItem lockItem = map.get(pageId);
        assert lockItem != null;
        lockItem.lock.lock();
        try {
            lockItem.tidInfos.remove(tid); // 释放掉即可
        } finally {
            lockItem.lock.unlock();
        }
    }

    public boolean holdsPage(TransactionId tid, PageId pageId) {
        map.putIfAbsent(pageId, new LockItem(new ReentrantLock(), new HashMap<>()));
        LockItem lockItem = map.get(pageId);
        assert lockItem != null;
        lockItem.lock.lock();
        try {
            return lockItem.tidInfos.containsKey(tid);
        } finally {
            lockItem.lock.unlock();
        }
    }

    public void releaseTransactionAllPages(TransactionId tid) {
        for (LockItem item : map.values()) { // weakly consistent，这里不影响。该方法应当只在事务结束时调用。
            item.lock.lock();
            try {
                item.tidInfos.remove(tid);
            } finally {
                item.lock.unlock();
            }
        }
    }

}
