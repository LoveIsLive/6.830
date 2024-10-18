package simpledb;

import simpledb.common.Type;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MyTest {
    public static void main(String[] args) {
        f();
    }

    public static void f() throws RuntimeException {
        System.out.println(null instanceof Object);
    }
}
