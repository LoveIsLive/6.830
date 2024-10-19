package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.exception.RuntimeReadIOException;
import simpledb.transaction.PageRWLockManage;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.transaction.TransactionPageLockManage;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private static class LRUDNode {
        PageId key;
        Page val;
        LRUDNode prev;
        LRUDNode next;

        public LRUDNode(PageId key, Page val) {
            this.key = key;
            this.val = val;
        }
        public LRUDNode() { }
    }
    // 实现LRU算法
    private final int numPages;
    private final LRUDNode head;
    private final LRUDNode tail;
    private final Map<PageId, LRUDNode> map;
    private final ReentrantLock mapMonitor = new ReentrantLock();
    // 对map操作加锁，也就意味着对LRU队列操作加锁
    private final TransactionPageLockManage tpLockManage;
    private final PageRWLockManage pageRWLockManage;
    private static final int TIMEOUT_MILLISECONDS = 1500; // 认为死锁超时的时间
    private final Random retryRandom = new Random();
    private final Random randomTimeout = new Random();
    private final Map<Long, Integer> tRandomTimeout = new HashMap<>();

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // completed!
        this.numPages = numPages;
        this.map = new HashMap<>();
        head = new LRUDNode();
        tail = new LRUDNode();
        head.next = tail;
        tail.prev = head;
        this.pageRWLockManage = new PageRWLockManage();
        this.tpLockManage = new TransactionPageLockManage(this.pageRWLockManage);
    }

    public TransactionPageLockManage getTpLockManage() {
        return tpLockManage;
    }

    public PageRWLockManage getPageRWLockManage() {
        return pageRWLockManage;
    }

    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // completed!
        long startTime = System.currentTimeMillis();
        boolean success = true;
        while (true) {
            success = true;
            mapMonitor.lock(); // map lock!
            LRUDNode node = map.get(pid);
            if(node == null) { // insert
                node = lruInsertHead(new LRUDNode(pid,
                        Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid)));
            } else {
                node.prev.next = node.next;
                node.next.prev = node.prev;
                node.next = head.next;
                head.next.prev = node;
                head.next = node;
                node.prev = head;
            }
            TransactionPageLockManage.LockInfo curLock = tpLockManage.holdTPLock(tid, pid); // 确切持有锁。（事务在新插入的页面也可能持有锁）
            StampedLock curStampLock = pageRWLockManage.getRWLock(pid);
            if(curLock != null) {
                // 如果所需写锁且当前不是，若只有当前事务拥有锁（且是读锁）则升级为写锁
                if(perm == Permissions.READ_WRITE && curLock.getPerm() == Permissions.READ_ONLY) {
                    Map<TransactionId, TransactionPageLockManage.LockInfo> tls = tpLockManage.getAllTransactionAndLock(pid);
                    if(tls.size() == 1) { // 仅当前事务拥有，升级锁。
                        long newStamp = curStampLock.tryConvertToWriteLock(curLock.getStamp()); // 必定成功
                        if(newStamp == 0) {
                            throw new RuntimeException("升级锁失败, 出现未知情况错误");
                        }
                        curLock.setStamp(newStamp);
                        curLock.setPerm(perm);
                        mapMonitor.unlock();
                        System.out.println("事务" + tid.getId()  + ": 升级页面 " + pid.getPageNumber() + " 的锁");
                    } else {
                        mapMonitor.unlock();
                        // 重试（等待至可以升级锁），超过一定时间认为发生了死锁
                        success = false;
                        if(System.currentTimeMillis() - startTime > getTransactionTimeout(tid.getId()))
                            throw new TransactionAbortedException(); // 认为死锁
                    }
                } else {
                    // 不需要做什么
                    mapMonitor.unlock();
                }
            } else {
                mapMonitor.unlock();
                long stamp = 0;
                if((stamp = accessLock(curStampLock, perm,200 + retryRandom.nextInt(50), TimeUnit.MILLISECONDS)) != 0) {
                    tpLockManage.addTPLock(tid, pid, stamp, perm);
                    System.out.println("事务" + tid.getId() + ": 获得页面 " + pid.getPageNumber() + " 的" + perm + "锁");
                } else {
                    success = false;
                    if(System.currentTimeMillis() - startTime > getTransactionTimeout(tid.getId()))
                        throw new TransactionAbortedException(); // 认为死锁
                }
            }
            if(success) {
                return node.val;
            }
        }
    }

    // 插入一个新节点
    private LRUDNode lruInsertHead(LRUDNode node) throws DbException {
        if(map.size() >= numPages) { // 插入前先驱逐
            evictPage();
        }
        map.put(node.key, node);
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
        node.prev = head;
        return node;
    }

    // 尝试以指定时间访问锁
    private long accessLock(StampedLock stampedLock, Permissions perm, long time, TimeUnit timeUnit) {
        try {
            return perm == Permissions.READ_ONLY ? stampedLock.tryReadLock(time, timeUnit) :
                    stampedLock.tryWriteLock(time, timeUnit);
        } catch (InterruptedException e) {
            System.out.println("accessLock fail: " + e.getMessage());
        }
        return 0; // 表示失败
    }

    // 随机化超时时间
    private synchronized int getTransactionTimeout(long tid) {
        tRandomTimeout.putIfAbsent(tid, randomTimeout.nextInt(1000) + TIMEOUT_MILLISECONDS);
        return tRandomTimeout.get(tid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // completed!
        // not necessary for lab1|lab2
        tpLockManage.removeTPLock(tid, pid);
    }


    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // completed!
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // completed!
        // not necessary for lab1|lab2
        return tpLockManage.holdTPLock(tid, p) != null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        System.out.println("commit: " + tid.getId() + " " + commit);
        if(commit) {
            try {
                // 刷新脏页
                flushPages(tid);
                // 释放事务的锁
                tpLockManage.removeAllTPLock(tid);
            } catch (IOException e) {
                throw new RuntimeException("unknown error: " + e.getMessage());
            }
        } else {
            discardPages(tid);
            tpLockManage.removeAllTPLock(tid);
        }
        tRandomTimeout.remove(tid.getId());
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // completed!
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);
        writeDirtyPagesToBP(dirtyPages);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // completed!
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        writeDirtyPagesToBP(dirtyPages);
    }

    private void writeDirtyPagesToBP(List<Page> dirtyPages) throws DbException {
        mapMonitor.lock();
        try {
            for (Page dirtyPage : dirtyPages) {
                PageId pid = dirtyPage.getId();
                // 不在bp中了
                if(!(map.containsKey(pid) && map.get(pid).val == dirtyPage)) {
                    lruInsertHead(new LRUDNode(dirtyPage.getId(), dirtyPage));
                }
            }
        } finally {
            mapMonitor.unlock();
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // completed!
        mapMonitor.lock();
        try {
            for (PageId pageId : map.keySet()) {
                flushPage(pageId);
            }
        } finally {
            mapMonitor.unlock();
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // completed!
        mapMonitor.lock();
        try {
            LRUDNode node = map.get(pid);
            node.prev.next = node.next;
            node.next.prev = node.prev;
            map.remove(pid);
        } finally {
            mapMonitor.unlock();
        }
    }
    // 废弃指定事务所弄脏的每一个页面
    public synchronized void discardPages(TransactionId tid) {
        mapMonitor.lock();
        try {
            LRUDNode p = head.next;
            while (p != tail) {
                Page page = p.val;
                if(page.isDirty() == tid) {
                    discardPage(page.getId());
                }
                p = p.next;
            }
        } finally {
            mapMonitor.unlock();
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // completed!
        mapMonitor.lock();
        try {
            LRUDNode lrudNode = map.get(pid);
            if(lrudNode == null) return;
            TransactionId dirtyTId = lrudNode.val.isDirty();
            if(dirtyTId != null) {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                dbFile.writePage(lrudNode.val);
                lrudNode.val.markDirty(false, dirtyTId);
            }
        } finally {
            mapMonitor.unlock();
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // completed!
        // not necessary for lab1|lab2
        mapMonitor.lock();
        try {
            LRUDNode p = head.next;
            while (p != tail) {
                Page page = p.val;
                if(page.isDirty() == tid) {
                    flushPage(page.getId());
                }
                p = p.next;
            }
        } finally {
            mapMonitor.unlock();
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // completed!
        mapMonitor.lock();
        try {
            LRUDNode p = tail.prev;
            while (p != head) {
                Page page = p.val;
                if(page.isDirty() == null) { // 同时不能驱逐带有写锁得页面，写锁视为脏页
                    map.remove(p.key);
                    p.prev.next = p.next;
                    p.next.prev = p.prev;
                    // 驱逐clean page时，不需要管页面上的事务和锁。
                    return;
                }
                p = p.prev;
            }
            throw new DbException("no clean page");
        } finally {
            mapMonitor.unlock();
        }
    }

}
