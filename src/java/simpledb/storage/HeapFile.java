package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.exception.RuntimeReadIOException;
import simpledb.exception.TypeMismatchException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
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
    private int numPages;
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
        return file.getAbsoluteFile().hashCode();
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
        numPages++;

        channel.close();
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
        // completed!
        BufferPool bufferPool = Database.getBufferPool();
        int tableId = getId();
        ArrayList<Page> res = new ArrayList<>(1);
        for (int i = 0; i < numPages; i++) {
            HeapPageId pageId = new HeapPageId(tableId, i);
            HeapPage page = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                res.add(page);
                return res;
            }
        }
        // 写入新页
        HeapPageId newPageId = new HeapPageId(tableId, numPages);
        writePage(new HeapPage(newPageId, HeapPage.createEmptyPageData())); // 经过bufferpool
        HeapPage page = (HeapPage) bufferPool.getPage(tid, newPageId, Permissions.READ_WRITE);
        page.insertTuple(t);
        page.markDirty(true, tid);
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // completed!
        HeapPage page = (HeapPage) Database.getBufferPool()
                .getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        ArrayList<Page> res = new ArrayList<>(1);
        res.add(page);
        return res;
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

