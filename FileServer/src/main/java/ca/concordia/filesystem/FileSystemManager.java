package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import javax.swing.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Arrays;

public class FileSystemManager {

    // ============================= PRIVATE VARIABLES ============================= //

    private FEntry[] fentryTable;
    private FNode[] fnodesTable;
    private FEntry[] inodeTable; // Array of inodes
    private boolean[] freeBlockList; // Bitmap for free blocks

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    //private static FileSystemManager instance;
    private RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();

    private static final int BLOCK_SIZE = 128; // Example block size
    private static final int FENTRY_BYTES = 15; // number of bytes for an FEntry
    private static final int FNODE_BYTES = 4; // number of bytes for an FNode

    private int fentryRegionBytes; //calculate total fentry bytes
    private int fnodeRegionBytes; // calculate total fnode bytes
    private int metadataBytes; // total metadata bytes
    private int firstDataBlockIndex; //offset to the first data block


    // ============================= PRIVATE FUNCTIONS ============================= //
    private void computeMetaDataBytes(){
        fentryRegionBytes = MAXFILES*FENTRY_BYTES;
        fnodeRegionBytes = MAXBLOCKS*FNODE_BYTES;
        metadataBytes = fentryRegionBytes + fnodeRegionBytes;
        firstDataBlockIndex = (metadataBytes + BLOCK_SIZE - 1)/BLOCK_SIZE; // first data block index
    }

    private int findFile(String name){
        for (int i = 0; i < MAXFILES; i++) {
            if(fentryTable[i] != null && fentryTable[i].getFilename().equals(name)) return i;
        }
        return -1;
    }
    private int findFreeSlot(){
        for (int i = 0; i < MAXFILES; i++) {
            if(fentryTable[i] == null) return i;
        }
        return -1;
    }

    private int countAvailableBlocks(){
        int count = 0;
        for (boolean f:freeBlockList) if(f) count++;
        return count;
    }


    public FileSystemManager(String filename, int totalSize) {
        // Initialize the file system manager with a file
        try{
            disk = new RandomAccessFile(filename, "rw");
            disk.setLength(totalSize);
            computeMetaDataBytes();
            System.out.println("First data block index: " + firstDataBlockIndex);
            System.out.println("Disk created with size: " + totalSize + " bytes");
            fentryTable = new FEntry[MAXFILES];
            fnodesTable = new FNode[MAXBLOCKS];
            freeBlockList = new boolean[MAXBLOCKS];

            for (int i = 0; i < MAXBLOCKS; i++) {
                freeBlockList[i] = true; //all blocks are free at the start
            }

            for (int i = 0; i < firstDataBlockIndex; i++) {
                freeBlockList[i] = false; // index up until first datablock is reserved for metadata
            }


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
