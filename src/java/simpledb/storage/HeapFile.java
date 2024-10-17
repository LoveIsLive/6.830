package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.exception.RuntimeReadIOException;
import simpledb.exception.TypeMismatchException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDesc;
    private volatile int numPages;
    private final int id;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // completed!
        this.file = f;
        this.tupleDesc = td;
        this.numPages = (int) (file.length() / BufferPool.getPageSize());
        this.id = file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // completed!
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // completed!
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // completed!
        return tupleDesc;
    }

    // open file and position offset
    private SeekableByteChannel openFileAndPosition(PageId pid, OpenOption... options) throws IOException {
        if(!(pid instanceof HeapPageId))
            throw new TypeMismatchException("HeapFile should be HeapPageId");
        if(!file.exists())
            throw new IllegalArgumentException("file is not exist");
        SeekableByteChannel channel = Files.newByteChannel(file.toPath(), options);
        HeapPageId heapPageId = (HeapPageId) pid;
        int pageNumber = heapPageId.getPageNumber();
        long offset = (long) pageNumber * BufferPool.getPageSize();
        channel.position(offset);
        return channel;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // completed!
        try {
            SeekableByteChannel channel = openFileAndPosition(pid, StandardOpenOption.READ);
            byte[] data = new byte[BufferPool.getPageSize()];
            channel.read(ByteBuffer.wrap(data));
            channel.close();

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeReadIOException("lab no read IOException");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // completed!
        SeekableByteChannel channel = openFileAndPosition(page.getId(), StandardOpenOption.WRITE);
        channel.write(ByteBuffer.wrap(page.getPageData()));
        if(channel instanceof FileChannel)
            ((FileChannel) channel).force(true); // flush

        channel.close();
    }

    // NOTE: sync
    private synchronized HeapPageId appendEmptyPage() throws IOException {
        HeapPageId newPageId = new HeapPageId(id, numPages);
        SeekableByteChannel channel = openFileAndPosition(newPageId, StandardOpenOption.WRITE);
        channel.write(ByteBuffer.wrap(HeapPage.createEmptyPageData()));
        if(channel instanceof FileChannel)
            ((FileChannel) channel).force(true); // flush

        numPages++;
        channel.close();
        return newPageId;
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // completed!
        return numPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // no completed!!!
        BufferPool bufferPool = Database.getBufferPool();
        int tableId = getId();
        ArrayList<Page> res = new ArrayList<>(1);
        int idx = 0;
        while (true) {
            if(idx == numPages)
                appendEmptyPage();
            HeapPageId pageId = new HeapPageId(tableId, idx);
            boolean prevHasLock = bufferPool.holdsLock(tid, pageId);
            HeapPage page = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_ONLY);
            // 如果有空槽请求写，没有则释放锁。
            if(page.getNumEmptySlots() > 0) {
                page = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_WRITE); // 再以写访问
                page.insertTuple(t);
                page.markDirty(true, tid);
                // 页面是否还在缓冲区，确保这个页面没有被驱逐（即确保写入）
                if(bufferPool.holdsPage(page)) {
                    res.add(page);
                    return res;
                } else if (!prevHasLock) {
                    System.out.println("insertTuple: 刚请求的页面被驱逐，写入没有正确写入，重试");
                    releaseLock(bufferPool, tid, pageId);
                }
                // 失败，继续访问当前页
            } else {
                // 之前本线程没有锁，才可以提前释放
                if(!prevHasLock) {
                    releaseLock(bufferPool, tid, pageId);
                }
                idx++; // 访问下一页
            }
        }
    }

    private void releaseLock(BufferPool bufferPool, TransactionId tid, PageId pageId) {
        try { // 可能被其他线程持有锁，这种情况下，不能移除锁。
            bufferPool.getTpLockManage().weakRemoveTPLock(tid, pageId);
        } catch (IllegalMonitorStateException e) {
            System.out.println("no-error: unlock other thread lock: " + e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // completed!
        HeapPage page = null;
        ArrayList<Page> res = new ArrayList<>(1);
        BufferPool bufferPool = Database.getBufferPool();
        PageId pageId = t.getRecordId().getPageId();
        while (true) {
            boolean prevHasLock = bufferPool.holdsLock(tid, pageId);
            page = (HeapPage)
                    bufferPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
            page.deleteTuple(t);
            page.markDirty(true, tid);
            if(bufferPool.holdsPage(page)) {
                res.add(page);
                return res;
            } else if (!prevHasLock) {
                System.out.println("deleteTuple: 刚请求的页面被驱逐，写入没有正确写入，重试");
                releaseLock(bufferPool, tid, pageId);
            }
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // completed!
        return new HeapFileIterator(this, tid);
    }
}

class HeapFileIterator extends AbstractDbFileIterator {
    private final HeapFile file;
    private final TransactionId tid;
    private Iterator<Tuple> curIterator;
    private PageId curPageId;

    public HeapFileIterator(HeapFile file, TransactionId tid) {
        this.file = file;
        this.tid = tid;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // completed!
        while (curIterator != null) {
            if(curIterator.hasNext()) return curIterator.next();
            curIterator = null;
            if (curPageId.getPageNumber() + 1 < file.numPages()) {
                curPageId = new HeapPageId(file.getId(), curPageId.getPageNumber() + 1);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY);
                curIterator = page.iterator();
            }
        }
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.curPageId = new HeapPageId(file.getId(), 0);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY);
        this.curIterator = page.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        curIterator = null;
        curPageId = null;
    }
}

