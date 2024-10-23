package simpledb;

import simpledb.common.Type;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.RandomAccess;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

public class MyTest {
    public static void main(String[] args) throws Exception {
        RandomAccessFile rf = new RandomAccessFile(new File("test.txt"), "rw");
        rf.getFD().sync();

    }

}
