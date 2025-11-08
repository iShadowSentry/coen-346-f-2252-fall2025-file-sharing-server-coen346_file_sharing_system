package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Arrays;

public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    //private static FileSystemManager instance;
    private RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();

    private static final int BLOCK_SIZE = 128; // Example block size

    private FEntry[] inodeTable; // Array of inodes
    private boolean[] freeBlockList; // Bitmap for free blocks

    public FileSystemManager(String filename, int totalSize) {
        // Initialize the file system manager with a file
        try{
            disk = new RandomAccessFile(filename, "rw");
            disk.setLength(totalSize);
            System.out.println("Disk created with size: " + totalSize + " bytes");
        } catch (IOException e){
            System.out.println("Disk could not be created.");
            e.printStackTrace();
        }
//        if(instance == null) {
//
//            //TODO Initialize the file system
//        } else {
//            throw new IllegalStateException("FileSystemManager is already initialized.");
//        }

    }

    public void createFile(String fileName) throws Exception {
        FileSystemManager newFile = new FileSystemManager(fileName, 5);
        // TODO
        //throw new UnsupportedOperationException("Method not implemented yet.");
    }


    // TODO: Add readFile, writeFile and other required methods,
}
