package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemManager {

    // ============================= PRIVATE VARIABLES ============================= //

    private FEntry[] fentryTable;
    private FNode[] fnodesTable;
    private boolean[] freeBlockList;

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;

    private RandomAccessFile disk;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock  = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private static final int BLOCK_SIZE = 128;  // size of each block
    private static final int FENTRY_BYTES = 15; // bytes per FEntry
    private static final int FNODE_BYTES = 4;   // bytes per FNode

    private int fentryRegionBytes;   // total bytes for fentry array
    private int fnodeRegionBytes;    // total bytes for fnode array
    private int metadataBytes;       // total metadata bytes (fentry + fnode)
    private int firstDataBlockIndex; // index of first data block
    private int readersCount = 0;

    // ============================= PRIVATE HELPERS ============================= //

    private void computeMetaDataBytes() {
        fentryRegionBytes = MAXFILES * FENTRY_BYTES;
        fnodeRegionBytes  = MAXBLOCKS * FNODE_BYTES;
        metadataBytes     = fentryRegionBytes + fnodeRegionBytes;
        // which block index does data start at?
        firstDataBlockIndex = (metadataBytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
    }

    private int findFile(String name) {
        for (int i = 0; i < MAXFILES; i++) {
            if (fentryTable[i] != null && fentryTable[i].getFilename().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private int findFreeSlot() {
        for (int i = 0; i < MAXFILES; i++) {
            if (fentryTable[i] == null) return i;
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

    private int blocksNeeded(int n) {
        if (n <= 0) return 0;
        return (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    }

    private java.util.List<Integer> findFreeDataBlocks(int n) {
        java.util.List<Integer> out = new java.util.ArrayList<>();
        for (int i = firstDataBlockIndex; i < MAXBLOCKS && out.size() < n; i++) {
            if (freeBlockList[i]) out.add(i);
        }
        return out.size() == n ? out : java.util.List.of();
    }

    private java.util.List<Integer> findFreeNodeIndecies(int n) {
        java.util.List<Integer> out = new java.util.ArrayList<>();
        for (int i = 0; i < MAXBLOCKS && out.size() < n; i++) {
            if (fnodesTable[i] == null) out.add(i);
        }
        return out.size() == n ? out : java.util.List.of();
    }

    private void rebuildFreeBlocks() {
        Arrays.fill(freeBlockList, true);

        // metadata blocks are not free
        for (int i = 0; i < firstDataBlockIndex; i++) {
            freeBlockList[i] = false;
        }

        // any block used by an FNode is not free
        for (FNode fn : fnodesTable) {
            if (fn != null) {
                int block = fn.getBlockIndex();
                freeBlockList[block] = false;
            }
        }
    }

    private void loadMetaData() throws IOException {
        // ---------- Load FEntries ----------
        for (int i = 0; i < MAXFILES; i++) {
            disk.seek(offsetOfFEntry(i));

            byte[] raw = new byte[FENTRY_BYTES];
            disk.read(raw);

            boolean empty = true;
            for (byte b : raw) {
                if (b != 0) {
                    empty = false;
                    break;
                }
            }
            if (empty) {
                fentryTable[i] = null;
                continue;
            }

            int end = 0;
            while (end < 11 && raw[end] != 0) {
                end++;
            }

            String name = new String(raw, 0, end);

            short size  = (short) (((raw[11] & 0xFF) << 8) | (raw[12] & 0xFF));
            short first = (short) (((raw[13] & 0xFF) << 8) | (raw[14] & 0xFF));

            fentryTable[i] = new FEntry(name, size, first);
        }

        for (int i = 0; i < MAXBLOCKS; i++) {
            disk.seek(offsetOfFNode(i));

            byte[] raw = new byte[FNODE_BYTES];
            disk.read(raw);

            boolean empty = true;
            for (byte b : raw) {
                if (b != 0) {
                    empty = false;
                    break;
                }
            }
            if (empty) {
                fnodesTable[i] = null;
                continue;
            }

            short block = (short) (((raw[0] & 0xFF) << 8) | (raw[1] & 0xFF));
            short next  = (short) (((raw[2] & 0xFF) << 8) | (raw[3] & 0xFF));

            FNode node = new FNode(block);
            node.setNext(next);
            fnodesTable[i] = node;
        }
    }

    private void saveMetaData() throws IOException {
        // ---------- Save FEntries ----------
        for (int i = 0; i < MAXFILES; i++) {
            disk.seek(offsetOfFEntry(i));

            FEntry fe = fentryTable[i];
            byte[] raw = new byte[FENTRY_BYTES];

            if (fe != null) {
                byte[] name = fe.getFilename().getBytes();
                int length = Math.min(name.length, 11);
                System.arraycopy(name, 0, raw, 0, length);

                raw[11] = (byte) (fe.getFilesize() >> 8);
                raw[12] = (byte) (fe.getFilesize());
                raw[13] = (byte) (fe.getFirstBlock() >> 8);
                raw[14] = (byte) (fe.getFirstBlock());
            }

            disk.write(raw);
        }

        // ---------- Save FNodes ----------
        for (int i = 0; i < MAXBLOCKS; i++) {
            disk.seek(offsetOfFNode(i));

            FNode fn = fnodesTable[i];
            byte[] raw = new byte[FNODE_BYTES];

            if (fn != null) {
                raw[0] = (byte) (fn.getBlockIndex() >> 8);
                raw[1] = (byte) (fn.getBlockIndex());
                raw[2] = (byte) (fn.getNext() >> 8);
                raw[3] = (byte) (fn.getNext());
            }
            disk.write(raw);
        }
    }

    public FileSystemManager(String filename, int totalSize) {
        try {
            disk = new RandomAccessFile(filename, "rw");
            disk.setLength(totalSize);
            computeMetaDataBytes();
            System.out.println("First data block index: " + firstDataBlockIndex);
            System.out.println("Disk created with size: " + totalSize + " bytes");

            fentryTable   = new FEntry[MAXFILES];
            fnodesTable   = new FNode[MAXBLOCKS];
            freeBlockList = new boolean[MAXBLOCKS];

            loadMetaData();
            rebuildFreeBlocks();

        } catch (IOException e) {
            System.out.println("Disk could not be created.");
            e.printStackTrace();
        }
    }

    public void createFile(String fileName) throws Exception {
        if (fileName == null || fileName.isEmpty()) {
            throw new Exception("File name is null or empty.");
        }

        writeLock.lock();
        try {
            if (findFile(fileName) != -1) {
                throw new Exception("File already exists.");
            }

            int fileSlot = findFreeSlot();
            if (fileSlot == -1) {
                throw new Exception("No free slot found.");
            }

            FEntry newFile = new FEntry(fileName, (short) 0, (short) -1);
            fentryTable[fileSlot] = newFile;
            saveMetaData();
            System.out.println("Created File: " + fileName);
        } finally {
            writeLock.unlock();
        }
    }

    public void writeFile(String filename, byte[] contents) throws Exception {
        int fileIndex = findFile(filename);
        if (fileIndex == -1)
            throw new Exception("ERROR: file " + filename + " does not exist");

        writeLock.lock();
        try {
            FEntry entry = fentryTable[fileIndex];

            // ---------- Case: writing 0 bytes -> clear file ----------
            if (contents.length == 0) {
                short nodeIndex = entry.getFirstBlock();

                while (nodeIndex != -1) {
                    FNode node = fnodesTable[nodeIndex];
                    if (node == null) break;

                    int block = node.getBlockIndex();
                    if (block >= 0 && block < freeBlockList.length) {
                        long off = offsetOfBlock(block);
                        disk.seek(off);
                        disk.write(new byte[BLOCK_SIZE]); // zero out
                        freeBlockList[block] = true;
                    }
                    int next = node.getNext();
                    fnodesTable[nodeIndex] = null;
                    nodeIndex = (short) next;
                }

                entry.setFirstBlock((short) -1);
                entry.setFilesize((short) 0);
                saveMetaData();
                System.out.println("Wrote 0 Bytes to " + filename + " (cleared)");
                return;
            }

            // ---------- Allocate new blocks & nodes ----------
            int requireBlocks = blocksNeeded(contents.length);

            var freeBlocks = findFreeDataBlocks(requireBlocks);
            if (freeBlocks.isEmpty())
                throw new Exception("ERROR: file too large (no free blocks)");

            var freeNodes = findFreeNodeIndecies(requireBlocks);
            if (freeNodes.isEmpty())
                throw new Exception("ERROR: no free FNODE slots");

            java.util.List<Integer> newNodesUsed  = new java.util.ArrayList<>();
            java.util.List<Integer> newBlocksUsed = new java.util.ArrayList<>();
            short newHead = -1;
            short prevNode = -1;

            try {
                int remaining = contents.length;
                int cursor    = 0;

                for (int i = 0; i < requireBlocks; i++) {
                    int nodeIndex = freeNodes.get(i);
                    int block     = freeBlocks.get(i);

                    int chunk = Math.min(remaining, BLOCK_SIZE);
                    long off  = offsetOfBlock(block);
                    disk.seek(off);
                    disk.write(contents, cursor, chunk);
                    if (chunk < BLOCK_SIZE) {
                        disk.write(new byte[BLOCK_SIZE - chunk]); // pad rest with zeros
                    }

                    FNode node = new FNode(block);
                    node.setNext(-1);
                    fnodesTable[nodeIndex] = node;

                    if (prevNode == -1) {
                        newHead = (short) nodeIndex;
                    } else {
                        fnodesTable[prevNode].setNext(nodeIndex);
                    }
                    prevNode = (short) nodeIndex;

                    newNodesUsed.add(nodeIndex);
                    newBlocksUsed.add(block);

                    freeBlockList[block] = false;
                    remaining -= chunk;
                    cursor    += chunk;
                }

                // ---------- Free old chain AFTER new one is safely written ----------
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
                saveMetaData();
                System.out.println("Wrote " + contents.length + " Bytes to " + filename +
                        " using " + requireBlocks + " blocks.");

            } catch (Exception ex) {
                // rollback
                for (int i = 0; i < newNodesUsed.size(); i++) {
                    int nodeIndex = newNodesUsed.get(i);
                    int block     = newBlocksUsed.get(i);
                    try {
                        long off = offsetOfBlock(block);
                        disk.seek(off);
                        disk.write(new byte[BLOCK_SIZE]);
                    } catch (Exception ignore) {}
                    fnodesTable[nodeIndex] = null;
                    if (block >= 0 && block < freeBlockList.length) freeBlockList[block] = true;
                }
                throw ex;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public byte[] readFile(String filename) throws Exception {
        int fileIndex = findFile(filename);
        if (fileIndex == -1)
            throw new Exception("ERROR: file " + filename + " does not exist");

        readLock.lock();
        try {
            FEntry entry = fentryTable[fileIndex];
            if (entry.getFilesize() == 0 || entry.getFirstBlock() == -1)
                return new byte[0];

            byte[] output = new byte[entry.getFilesize()];
            int written = 0;

            short nodeIndex = entry.getFirstBlock();
            while (nodeIndex != -1 && written < entry.getFilesize()) {
                FNode node = fnodesTable[nodeIndex];
                if (node == null) break;

                int block = node.getBlockIndex();
                long off  = offsetOfBlock(block);
                disk.seek(off);

                int toRead = Math.min(entry.getFilesize() - written, BLOCK_SIZE);
                disk.readFully(output, written, toRead);

                written   += toRead;
                nodeIndex = (short) node.getNext();
            }

            return output;
        } finally {
            readLock.unlock();
        }
    }

    public void deleteFile(String fileName) throws Exception {
        int fileIndex = findFile(fileName);
        if (fileIndex == -1)
            throw new Exception("ERROR: file " + fileName + " does not exist");

        writeLock.lock();
        try {
            FEntry entry = fentryTable[fileIndex];

            short nodeIndex = entry.getFirstBlock();
            while (nodeIndex != -1) {
                FNode node = fnodesTable[nodeIndex];
                if (node == null) break;

                int block = node.getBlockIndex();
                if (block >= 0 && block < freeBlockList.length) {
                    long offset = offsetOfBlock(block);
                    disk.seek(offset);
                    disk.write(new byte[BLOCK_SIZE]); // zero out
                    freeBlockList[block] = true;
                }

                int next = node.getNext();
                fnodesTable[nodeIndex] = null;
                nodeIndex = (short) next;
            }

            fentryTable[fileIndex] = null;
            saveMetaData();
            System.out.println("Deleted file: " + fileName);
        } finally {
            writeLock.unlock();
        }
    }

    public String[] listFiles() {
        readLock.lock();
        try {
            java.util.List<String> names = new java.util.ArrayList<>();
            for (int i = 0; i < MAXFILES; i++) {
                if (fentryTable[i] != null) {
                    names.add(fentryTable[i].getFilename());
                }
            }
            return names.toArray(new String[0]);
        } finally {
            readLock.unlock();
        }
    }
}
