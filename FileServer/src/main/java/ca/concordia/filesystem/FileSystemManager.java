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

    private int blocksNeeded(int n){
        if (n <= 0) return 0;
        return (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    }
    
    private java.util.List<Integer> findFreeDataBlocks(int n){
        java.util.List<Integer> out = new java.util.ArrayList<>();
        for (int i = firstDataBlockIndex; i < MAXBLOCKS && out.size() < n; i++) {
            if (freeBlockList[i]) out.add(i);
        }
        return out.size() == n ? out : java.util.List.of();
    }

    private java.util.List<Integer> findFreeNodeIndecies(int n){
        java.util.List<Integer> out = new java.util.ArrayList<>();
        for (int i = 0; i < MAXBLOCKS && out.size() < n; i++) {
            if (fnodesTable[i] == null) out.add(i);
        }
        return  out.size() == n ? out : java.util.List.of();
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
        globalLock.lock();
        try {
            FEntry newFile = new FEntry(fileName, (short) 0, (short) -1);
            fentryTable[fileSlot] = newFile;
        }finally {
            globalLock.unlock();
        }
        System.out.println("Created File: " + fileName);
    }


    // TODO: Add readFile, writeFile and other required methods,

    public void writeFile(String filename, byte[] contents) throws Exception{
        int fileIndex = findFile(filename);
        if(fileIndex == -1) throw new Exception("ERROR: file " + filename + " does not exist");

//        if(contents.length > BLOCK_SIZE){
//            throw new Exception("ERROR: data too big for a single block");
//        }
        globalLock.lock();
        try{
            FEntry entry = fentryTable[fileIndex];
            if(contents.length == 0){
                short nodeIndex = entry.getFirstBlock();

                while (nodeIndex != -1) {
                    FNode node = fnodesTable[nodeIndex];
                    if (node == null) break;
                    int block = node.getBlockIndex();
                    if (block >= 0 && block < freeBlockList.length) {
                        long off = offsetOfBlock(block);
                        disk.seek(off);
                        disk.write(new byte[BLOCK_SIZE]);
                        freeBlockList[block] = true;
                    }
                    int next = node.getNext();
                    fnodesTable[nodeIndex] = null;
                    nodeIndex = (short) next;
                }
                entry.setFirstBlock((short) -1);
                entry.setFilesize((short) 0);
                System.out.println("Wrote 0 Bytes to " + filename + " (cleared)");
                return;
            }

            int requireBlocks = blocksNeeded(contents.length);

            var freeBlocks = findFreeDataBlocks(requireBlocks);
            if (freeBlocks.isEmpty()) throw new Exception("ERROR: file too large (no free blocks)");
            var freeNodes = findFreeNodeIndecies(requireBlocks);
            if (freeNodes.isEmpty()) throw new Exception("ERROR: no free FNODE slots");

            java.util.List<Integer> newNodesUsed = new java.util.ArrayList<>();
            java.util.List<Integer> newBlocksUsed = new java.util.ArrayList<>();
            short newHead = -1;
            short prevNode = -1;

            try {
                int remaining = contents.length;
                int cursor = 0;

                for (int i = 0; i < requireBlocks; i++){
                    int nodeIndex = freeNodes.get(i);
                    int block = freeBlocks.get(i);

                    int chunk = Math.min(remaining, BLOCK_SIZE);
                    long off = offsetOfBlock(block);
                    disk.seek(off);
                    disk.write(contents, cursor, chunk);
                    if (chunk < BLOCK_SIZE) {
                        disk.write(new byte[BLOCK_SIZE - chunk]);
                    }

                    FNode node = new FNode(block);
                    node.setNext(-1);
                    fnodesTable[nodeIndex] = node;

                    if (prevNode == -1) {
                        newHead = (short) nodeIndex;
                    }else {
                        fnodesTable[prevNode].setNext(nodeIndex);
                    }
                    prevNode = (short) nodeIndex;

                    newNodesUsed.add(nodeIndex);
                    newBlocksUsed.add(block);

                    freeBlockList[block] = false;
                    remaining -= chunk;
                    cursor += chunk;
                }

                short old = entry.getFirstBlock();
                while (old != -1) {
                    FNode node = fnodesTable[old];
                    if (node == null) break;
                    int blockIndex = node.getBlockIndex();
                    if (blockIndex >= 0 && blockIndex < freeBlockList.length) {
                        long off = offsetOfBlock(blockIndex);
                        disk.seek(off);
                        disk.write(new byte[BLOCK_SIZE]);
                        freeBlockList[blockIndex] = true;
                    }
                    int next = node.getNext();
                    fnodesTable[old] = null;
                    old = (short) next;
                }

                entry.setFirstBlock(newHead);
                entry.setFilesize((short) contents.length);
                System.out.println("Wrote " + contents.length + " Bytes to " + filename + " using " + requireBlocks + " blocks.");
            } catch (Exception ex){
                for (int i = 0; i < newNodesUsed.size(); i++) {
                    int nodeIndex = newNodesUsed.get(i);
                    int block = newBlocksUsed.get(i);
                    try {
                        long off = offsetOfBlock(block);
                        disk.seek(off);
                        disk.write(new byte[BLOCK_SIZE]);
                    } catch (Exception ignore) {}
                    fnodesTable[nodeIndex] = null;
                    if (block >= 0 && block < freeBlockList.length) freeBlockList[block] = true;
                }
                throw  ex;
            }
        } finally {
            globalLock.unlock();
        }

    }

    public byte[] readFile(String filename) throws Exception{
        int fileIndex = findFile(filename);
        if(fileIndex == -1) throw new Exception("ERROR: file " + filename + " does not exist");
        globalLock.lock();
        try{
            FEntry entry = fentryTable[fileIndex];
            if(entry.getFilesize() == 0 || entry.getFirstBlock() == -1) return new byte[0];

            byte[] output = new byte[entry.getFilesize()];
            int written = 0;

            short nodeIndex = entry.getFirstBlock();
            while (nodeIndex != -1 && written < entry.getFilesize()) {
                FNode node = fnodesTable[nodeIndex];
                if (node == null) break;

                int block = node.getBlockIndex();
                long off = offsetOfBlock(block);
                disk.seek(off);

                int toRead = Math.min(entry.getFilesize() - written, BLOCK_SIZE);
                disk.readFully(output, written, toRead);

                written += toRead;
                nodeIndex = (short) node.getNext();
            }
            return output;
        } finally {
            globalLock.unlock();
        }

//        FNode node = fnodesTable[entry.getFirstBlock()];
//        if(node == null){
//            throw new Exception("ERROR: file " + filename + " does not exist");
//        }
//
//        long offset = offsetOfBlock(node.getBlockIndex());
//        disk.seek(offset);
//        byte[] contents = new byte[Math.min(entry.getFilesize(), BLOCK_SIZE)];
//        disk.readFully(contents, 0, contents.length);
//        return contents;
    }

    public void deleteFile(String fileName) throws Exception {
        int fileIndex = findFile(fileName);
        if(fileIndex == -1) throw new Exception("ERROR: file " + fileName + " does not exist");

        globalLock.lock();
        try{
            FEntry entry = fentryTable[fileIndex];

            short nodeIndex = entry.getFirstBlock();
            while(nodeIndex != -1){
                FNode node = fnodesTable[nodeIndex];
                if(node == null) break;

                int block = node.getBlockIndex();
                if(block >= 0 && block < freeBlockList.length){
                    long offset = offsetOfBlock(block);
                    disk.seek(offset);
                    disk.write(new byte[BLOCK_SIZE]);
                    freeBlockList[block] = true;
                }

                int next = node.getNext();
                fnodesTable[nodeIndex] = null;
                nodeIndex = (short) next;

            }

            fentryTable[fileIndex] = null;
            System.out.println("Deleted file: " + fileName);
        }finally {
            globalLock.unlock();
        }


    }

    public String[] listFiles(){
        globalLock.lock();
        try{
            java.util.List<String> names = new java.util.ArrayList<>();
            for (int i = 0; i < MAXFILES; i++) {
                if(fentryTable[i] != null){
                    names.add(fentryTable[i].getFilename());
                }
            }
            return names.toArray(new String[0]);
        } finally {
            globalLock.unlock();
        }


    }

}
