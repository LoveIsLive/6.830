package simpledb;

import simpledb.common.Type;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

public class MyTest {
    public static void main(String[] args) throws Exception {
        StampedLock stampedLock = new StampedLock();
        long wStamp = stampedLock.writeLock();
        stampedLock.unlockWrite(wStamp);
//        new Thread(() -> {
//            try {
//                rwLock.writeLock().unlock();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }).start();

    }

}
