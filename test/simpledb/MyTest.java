package simpledb;

import simpledb.common.Type;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MyTest {
    public static void main(String[] args) throws Exception {
        SeekableByteChannel channel = Files.newByteChannel(Paths.get(URI.create("file:/D:/java_projects/simple-db/simple-db-hw-2021-master/src/java/simpledb/SimpleDb.java")), StandardOpenOption.READ);
        System.out.println(channel.getClass());
    }
}
