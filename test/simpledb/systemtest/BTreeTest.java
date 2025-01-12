package simpledb.systemtest;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.execution.IndexPredicate;
import simpledb.index.*;
import simpledb.storage.*;

import static org.junit.Assert.*;
import static simpledb.index.BTreeUtility.printAllPages;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.After;
import org.junit.Test;

import simpledb.index.BTreeUtility.*;
import simpledb.execution.Predicate.Op;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * System test for the BTree
 */
public class BTreeTest extends SimpleDbTestBase {
    private final static Random r = new Random();

    private static final int POLL_INTERVAL = 100;

    /**
     * Helper method to clean up the syntax of starting a BTreeInserter thread.
     * The parameters pass through to the BTreeInserter constructor.
     */
    public BTreeInserter startInserter(BTreeFile bf, int[] tupdata, BlockingQueue<List<Integer>> insertedTuples) {

        BTreeInserter bi = new BTreeInserter(bf, tupdata, insertedTuples);
        bi.start();
//        bi.run();
        return bi;
    }

    /**
     * Helper method to clean up the syntax of starting a BTreeDeleter thread.
     * The parameters pass through to the BTreeDeleter constructor.
     */
    public BTreeDeleter startDeleter(BTreeFile bf, BlockingQueue<List<Integer>> insertedTuples) {
        BTreeDeleter bd = new BTreeDeleter(bf, insertedTuples);
        bd.start();
//        bd.run();
        return bd;
    }

    private void waitForInserterThreads(List<BTreeInserter> insertThreads)
            throws Exception {
        Thread.sleep(POLL_INTERVAL);
        for (BTreeInserter thread : insertThreads) {
            int cnt = 0;
            while (!thread.succeeded() && thread.getError() == null) {
                cnt++;
                if(cnt == 50) { // 输出一次。可能发生问题！！！
                    System.out.println("waitForInserterThreads" + thread + "success: " + thread.succeeded() + ", error: " + thread.getError());
                }
                Thread.sleep(POLL_INTERVAL);
            }
        }
    }

    private void waitForDeleterThreads(List<BTreeDeleter> deleteThreads)
            throws Exception {
        Thread.sleep(POLL_INTERVAL);
        for (BTreeDeleter thread : deleteThreads) {
            int cnt = 0;
            while (!thread.succeeded() && thread.getError() == null) {
                cnt++;
                if(cnt == 50) { // 输出一次。可能发生问题！！！
                    System.out.println("waitForDeleterThreads" + thread + "success: " + thread.succeeded() + ", error: " + thread.getError());
                }
                Thread.sleep(POLL_INTERVAL);
            }
        }
    }

    private int[] getRandomTupleData() {
        int item1 = r.nextInt(BTreeUtility.MAX_RAND_VALUE);
        int item2 = r.nextInt(BTreeUtility.MAX_RAND_VALUE);
        return new int[]{item1, item2};
    }

    @After
    public void tearDown() {
        // set the page size back to the default
        BufferPool.resetPageSize();
        Database.reset();
    }

    /**
     * Test that doing lots of inserts and deletes in multiple threads works
     */
    @Test
    public void testBigFile() throws Exception {
        /*
        * internalPage和leafPage最多都是124项。
        *
        *
        * */
        // 重定位到文件
//        System.setOut(new PrintStream(Files.newOutputStream(Paths.get("输出.txt")), true));
//        System.setErr(new PrintStream(Files.newOutputStream(Paths.get("error.txt")), true));
        System.out.println(Thread.currentThread());

        // For this test we will decrease the size of the Buffer Pool pages
        BufferPool.setPageSize(1024);

        // This should create a B+ tree with a packed second tier of internal pages
        // and packed third tier of leaf pages
        System.out.println("Creating large random B+ tree...");
        List<List<Integer>> tuples = new ArrayList<>();
        BTreeFile bf = BTreeUtility.createRandomBTreeFile(2, 31000,
                null, tuples, 0);

        // we will need more room in the buffer pool for this test
        Database.resetBufferPool(500);

        BlockingQueue<List<Integer>> insertedTuples = new ArrayBlockingQueue<>(100000);
        insertedTuples.addAll(tuples);
        assertEquals(31000, insertedTuples.size());
        int size = insertedTuples.size();

        TransactionId repTId = new TransactionId();
        // now insert some random tuples
        System.out.println("Inserting tuples...");
        List<BTreeInserter> insertThreads = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            BTreeInserter bi = startInserter(bf, getRandomTupleData(), insertedTuples);
            insertThreads.add(bi);
            // The first few inserts will cause pages to split so give them a little
            // more time to avoid too many deadlock situations
            Thread.sleep(r.nextInt(POLL_INTERVAL));
        }

        for (int i = 0; i < 800; i++) {
            BTreeInserter bi = startInserter(bf, getRandomTupleData(), insertedTuples);
            insertThreads.add(bi);
        }

        // wait for all threads to finish
        waitForInserterThreads(insertThreads);

        System.out.println("check [0]");
        try {
            BTreeChecker.checkRep(bf, repTId, new HashMap<>(), true);
        } catch (Throwable e) {
            e.printStackTrace();
            printAllPages(bf, repTId);
            throw new RuntimeException(e);
        }
        Database.getBufferPool().transactionComplete(repTId, false);

        assertTrue(insertedTuples.size() > size);

        // now insert and delete tuples at the same time
        System.out.println("Inserting and deleting tuples...");
        List<BTreeDeleter> deleteThreads = new ArrayList<>();
        for (BTreeInserter thread : insertThreads) {
            thread.rerun(bf, getRandomTupleData(), insertedTuples);
            BTreeDeleter bd = startDeleter(bf, insertedTuples);
            deleteThreads.add(bd);
        }

        // wait for all threads to finish
        waitForInserterThreads(insertThreads);
        waitForDeleterThreads(deleteThreads);

        System.out.println("check [1]");
        try {
            BTreeChecker.checkRep(bf, repTId, new HashMap<>(), true);
        } catch (Throwable e) {
            e.printStackTrace();
            printAllPages(bf, repTId);
            throw new RuntimeException(e);
        }
        Database.getBufferPool().transactionComplete(repTId, false);

        int numPages = bf.numPages();
        size = insertedTuples.size();

        // now delete a bunch of tuples
        System.out.println("Deleting tuples...");
        for (int i = 0; i < 10; i++) {
            for (BTreeDeleter thread : deleteThreads) {
                thread.rerun(bf, insertedTuples); // 占用的主线程
            }

            // wait for all threads to finish
            waitForDeleterThreads(deleteThreads);
            System.out.println("check [2]");
            try {
                BTreeChecker.checkRep(bf, repTId, new HashMap<>(), true);
            } catch (Throwable e) {
                e.printStackTrace();
                printAllPages(bf, repTId);
                throw new RuntimeException(e);
            }
            Database.getBufferPool().transactionComplete(repTId, false);

        }
        assertTrue(insertedTuples.size() < size);
        size = insertedTuples.size();

        // now insert a bunch of random tuples again
        System.out.println("Inserting tuples...");
        for (int i = 0; i < 10; i++) {
            for (BTreeInserter thread : insertThreads) {
                thread.rerun(bf, getRandomTupleData(), insertedTuples);
            }

            // wait for all threads to finish
            waitForInserterThreads(insertThreads);
            System.out.println("check [3]");
            BTreeChecker.checkRep(bf, repTId, new HashMap<>(), true);
            Database.getBufferPool().transactionComplete(repTId, false);
        }
        assertTrue(insertedTuples.size() > size);
        size = insertedTuples.size();
        // we should be reusing the deleted pages
        assertTrue(bf.numPages() < numPages + 20);

        // kill all the threads
        insertThreads = null;
        deleteThreads = null;

        List<List<Integer>> tuplesList = new ArrayList<>(insertedTuples);
        DbFileIterator myIt = bf.iterator(new TransactionId());
        myIt.open();
        int myCnt = 0;
        while (myIt.hasNext()) {
            myIt.next();
            myCnt++;
        }
        myIt.close();
        System.out.println("expected num: " + tuplesList.size() + ", actual num: " + myCnt);

        TransactionId tid = new TransactionId();

        // First look for random tuples and make sure we can find them
        System.out.println("Searching for tuples...");
        for (int i = 0; i < 10000; i++) {
            int rand = r.nextInt(insertedTuples.size());
            List<Integer> tuple = tuplesList.get(rand);
            IntField randKey = new IntField(tuple.get(bf.keyField()));
            IndexPredicate ipred = new IndexPredicate(Op.EQUALS, randKey);
            DbFileIterator it = bf.indexIterator(tid, ipred);
            it.open();
            boolean found = false;
            while (it.hasNext()) {
                Tuple t = it.next();
                if (tuple.equals(SystemTestUtil.tupleToList(t))) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
            it.close();
        }

        // now make sure all the tuples are in order and we have the right number
        System.out.println("Performing sanity checks...");
        DbFileIterator it = bf.iterator(tid);
        Field prev = null;
        it.open();
        int count = 0;
        while (it.hasNext()) {
            Tuple t = it.next();
            if (prev != null) {
                assertTrue(t.getField(bf.keyField()).compare(Op.GREATER_THAN_OR_EQ, prev));
            }
            prev = t.getField(bf.keyField());
            count++;
        }
        it.close();
        assertEquals(count, tuplesList.size());
        Database.getBufferPool().transactionComplete(tid);

        // set the page size back
        BufferPool.resetPageSize();

    }

    /**
     * Make test compatible with older version of ant.
     */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(BTreeTest.class);
    }
}
