package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.exception.RuntimeReadIOException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.transaction.TransactionPageLockManage;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private static class TranLockItem {
        TransactionId tid;
        Lock lock;
        public TranLockItem(TransactionId tid, Lock lock) {
            this.tid = tid;
            this.lock = lock;
        }
    }
    private static class LRUDNode {
        PageId key;
        Page val;
        LRUDNode prev;
        LRUDNode next;
        Permissions perm;
        ReadWriteLock lock; // node上的读写锁

        public LRUDNode(PageId key, Page val) {
            this.key = key;
            this.val = val;
        }

        public LRUDNode(PageId key, Page val, TransactionId tid, Permissions perm, ReadWriteLock lock) {
            this.key = key;
            this.val = val;
            this.perm = perm;
            this.lock = lock;
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
    private final TransactionPageLockManage tpLockManage = new TransactionPageLockManage();


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
    }

    public TransactionPageLockManage getTpLockManage() {
        return tpLockManage;
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
        // no completed!!!
        mapMonitor.lock(); // map lock!
        LRUDNode node = map.get(pid);
        if(node == null) { // insert
            node = new LRUDNode(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid),
                    tid, perm, new ReentrantReadWriteLock());
            map.put(pid, node);
            node.next = head.next;
            head.next.prev = node;
            head.next = node;
            node.prev = head;
            if(map.size() > numPages) { // evict
                evictPage();
            }
        } else {
            node.prev.next = node.next;
            node.next.prev = node.prev;
            node.next = head.next;
            head.next.prev = node;
            head.next = node;
            node.prev = head;
        }
        List<Lock> curLock = tpLockManage.holdTPLock(tid, pid);
        if(curLock != null) {
            // 如果所需写锁且当前不是，若只有当前事务拥有锁（且是读锁）则升级为写锁，否则再请求写锁
            if(perm == Permissions.READ_WRITE && curLock.size() < 2 &&
                    curLock.get(0) instanceof ReentrantReadWriteLock.ReadLock) {
                Map<TransactionId, List<Lock>> tls = tpLockManage.getAllTransactionAndLock(pid);
                if(tls.size() == 1) { // 仅当前事务拥有，升级锁
                    tpLockManage.removeTPLock(tid, pid); // 直接抛弃之前的锁
                    ReentrantReadWriteLock newLock = new ReentrantReadWriteLock();
                    newLock.writeLock().lock();
                    tpLockManage.addTPLock(tid, pid, newLock.writeLock());
                    node.lock = newLock;
                    mapMonitor.unlock();
                } else {
                    tpLockManage.addTPLock(tid, pid, node.lock.writeLock());
                    mapMonitor.unlock();
                    node.lock.writeLock().lock();
                }
            } else {
                mapMonitor.unlock();
            }
        } else {
            // 首先声明页面被当前事务持有
            tpLockManage.addTPLock(tid, pid,
                    perm == Permissions.READ_ONLY ? node.lock.readLock() : node.lock.writeLock());
            mapMonitor.unlock();
            if(perm == Permissions.READ_ONLY) {
                node.lock.readLock().lock();
            } else {
                node.lock.writeLock().lock();
            }
        }
        return node.val;
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
        tpLockManage.releaseTPLock(tid, pid);
        tpLockManage.removeTPLock(tid, pid);
    }


    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
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
        dbFile.insertTuple(tid, t);
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
        dbFile.deleteTuple(tid, t);
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
        // no-completed!
        // not necessary for lab1|lab2
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
                    tpLockManage.discardAllPageLock(page.getId());
                    return;
                }
                p = p.prev;
            }
            throw new DbException("no clear page");
        } finally {
            mapMonitor.unlock();
        }
    }

}
