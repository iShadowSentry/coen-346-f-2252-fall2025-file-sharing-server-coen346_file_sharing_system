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

    private int findFreeDataBlock(){
        for (int i = firstDataBlockIndex; i < MAXBLOCKS; i++) {
            if(freeBlockList[i]) return i;
        }
        return -1;
    }

    private int findFreeNode(){
        for (int i = 0; i < MAXBLOCKS; i++) {
            if(fnodesTable[i] == null) return i;
        }
        return -1;
    }

    private long offsetOfFEntry(int i) {
        return (long) i * FENTRY_BYTES;
    }

    private long offsetOfFNode(int j) {
        return (long) fentryRegionBytes + (long) j * FNODE_BYTES;
    }

    private long offsetOfBlock(int blockIndex) {
        return (long) blockIndex * BLOCK_SIZE;
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
//        } else {
//            throw new IllegalStateException("FileSystemManager is already initialized.");
//        }

    }

    public void createFile(String fileName) throws Exception {

        if (fileName == null || fileName.isEmpty()){
            throw new Exception("File name is null or empty.");
        }

        if (findFile(fileName)!=-1){
            throw new Exception("File already exists.");
        }

        int fileSlot = findFreeSlot();
        if (fileSlot == -1){
            throw new Exception("No free slot found.");
        }

        FEntry newFile = new FEntry(fileName, (short) 0, (short) -1);
        fentryTable[fileSlot] = newFile;
        System.out.println("Created File: " + fileName);

       // throw new UnsupportedOperationException("Method not implemented yet.");
    }


    // TODO: Add readFile, writeFile and other required methods,

    public void writeFile(String filename, byte[] contents) throws Exception{
        int fileIndex = findFile(filename);
        if(fileIndex == -1) throw new Exception("ERROR: file " + filename + " does not exist");

        if(contents.length > BLOCK_SIZE){
            throw new Exception("ERROR: data too big for a single block");
        }

        FEntry entry = fentryTable[fileIndex];
        if(entry.getFirstBlock() != -1){
            FNode old = fnodesTable[entry.getFirstBlock()];
            if(old != null && old.getBlockIndex()>=0){
                long off = offsetOfBlock(old.getBlockIndex());
                disk.seek(off);
                disk.write(new byte[BLOCK_SIZE]);
                freeBlockList[old.getBlockIndex()] = true;
            }
            fnodesTable[entry.getFirstBlock()] = null;
            entry.setFirstBlock((short) -1);
        }

        int block = findFreeDataBlock();
        if(block == -1) {
            throw new Exception("ERROR: file too large (no free blocks)");
        }

        long offset = offsetOfBlock(block);
        disk.seek(offset);
        disk.write(contents);
        if(contents.length < BLOCK_SIZE){
            disk.write(new byte[BLOCK_SIZE - contents.length]);
        }

        int nodeIndex = findFreeNode();
        if(nodeIndex == -1) throw new Exception("ERROR: no free FNode slots");

        FNode node = new FNode(block);
        node.setNext(-1);
        fnodesTable[nodeIndex] = node;

        entry.setFirstBlock((short) nodeIndex);
        entry.setFilesize((short) contents.length);
        freeBlockList[block] = false;

        System.out.println("Wrote " + contents.length + " bytes to " + filename + " in block " + block);
    }

    public byte[] readfile(String filename) throws Exception{
        int fileIndex = findFile(filename);
        if(fileIndex == -1) throw new Exception("ERROR: file " + filename + " does not exist");
        FEntry entry = fentryTable[fileIndex];
        if(entry.getFilesize() == 0 || entry.getFirstBlock() == -1) return new byte[0];

        FNode node = fnodesTable[entry.getFirstBlock()];
        if(node == null){
            throw new Exception("ERROR: file " + filename + " does not exist");
        }

        long offset = offsetOfBlock(node.getBlockIndex());
        disk.seek(offset);
        byte[] contents = new byte[Math.min(entry.getFilesize(), BLOCK_SIZE)];
        disk.readFully(contents, 0, contents.length);
        return contents;
    }
}
