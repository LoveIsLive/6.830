package simpledb.transaction;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/*
    事务-页面-锁的管理，一个事务在一个页面某一时刻只能拥有一个锁（读锁 or 写锁）
    NOTE: 事务-页面，页面不一定在缓冲区（由于驱逐clean page）

    如果使用ReentrantReadWriteLock: 假设一个事务仅在一个线程下处理。否则unlock时会出错。（无法通过DeadlockTest和BTreeDeadlockTest单元测试）
    改用StampedLock.
*/

public class TransactionPageLockManage {
    public static class LockInfo {
        private long stamp; // 事务获取的stamp
        private Permissions perm; // 事务获取的权限(可以不使用到这个属性)

        public LockInfo(long stamp, Permissions perm) {
            this.stamp = stamp;
            this.perm = perm;
        }

        public long getStamp() {
            return stamp;
        }

        public void setStamp(long stamp) {
            this.stamp = stamp;
        }

        public Permissions getPerm() {
            return perm;
        }

        public void setPerm(Permissions perm) {
            this.perm = perm;
        }

        @Override
        public String toString() {
            return "LockInfo{" +
                    "stamp=" + stamp +
                    ", perm=" + perm +
                    '}';
        }
    }

    private final Map<TransactionId, Map<PageId, LockInfo>> tpMap = new HashMap<>();
    private final Map<PageId, Map<TransactionId, LockInfo>> ptMap = new HashMap<>();
    private final PageRWLockManage pageRWLockManage;

    public TransactionPageLockManage(PageRWLockManage pageRWLockManage) {
        this.pageRWLockManage = pageRWLockManage;
    }

    public synchronized void addTPLock(TransactionId tid, PageId pageId, long stamp, Permissions perm) {
        LockInfo lockInfo = new LockInfo(stamp, perm);
        tpMap.putIfAbsent(tid, new HashMap<>());
        tpMap.get(tid).put(pageId, lockInfo);

        ptMap.putIfAbsent(pageId, new HashMap<>());
        ptMap.get(pageId).put(tid, lockInfo);
    }

    // 移除事务在该页面上的锁
    public synchronized void removeTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, LockInfo> nodeMap = tpMap.get(tid);
        if(nodeMap != null) {
            LockInfo lockInfo = nodeMap.get(pageId);
            pageRWLockManage.getRWLock(pageId).unlock(lockInfo.stamp);
            nodeMap.remove(pageId);
        }
        Map<TransactionId, LockInfo> map = ptMap.get(pageId);
        if(map != null) map.remove(tid);
    }

    // 移除事务在所有页面上的锁
    public synchronized void removeAllTPLock(TransactionId tid) {
        Map<PageId, LockInfo> nodeMap = tpMap.get(tid);
        if(nodeMap == null) return;
        for (Map.Entry<PageId, LockInfo> entry : nodeMap.entrySet()) {
            pageRWLockManage.getRWLock(entry.getKey()).unlock(entry.getValue().stamp);
        }
        tpMap.remove(tid);
        for (Map<TransactionId, LockInfo> value : ptMap.values()) {
            value.remove(tid);
        }
    }

    // 返回事务在该页面上持有的锁
    public synchronized LockInfo holdTPLock(TransactionId tid, PageId pageId) {
        Map<PageId, LockInfo> nodeMap = tpMap.get(tid);
        if(nodeMap == null) return null;
        return nodeMap.get(pageId);
    }

    // 返回所有锁定该页面的事务和锁
    public synchronized Map<TransactionId, LockInfo> getAllTransactionAndLock(PageId pageId) {
        return ptMap.get(pageId);
    }

}
