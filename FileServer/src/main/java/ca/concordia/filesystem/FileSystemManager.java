package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemManager {

    // =================== CONSTANTS ===================
    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;

    private static final int BLOCK_SIZE = 128;
    private static final int FENTRY_BYTES = 15;
    private static final int FNODE_BYTES = 4;

    // =================== STRUCTURES ===================
    private FEntry[] fentryTable;
    private FNode[] fnodesTable;
    private boolean[] freeBlockList;

    private RandomAccessFile disk;

    private int fentryRegionBytes;
    private int fnodeRegionBytes;
    private int metadataBytes;
    private int firstDataBlockIndex;

    // =================== THREAD SAFETY ===================
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true); // fair lock

    // =================== CONSTRUCTOR ===================
    public FileSystemManager(String filename, int totalSize) {
        try {
            disk = new RandomAccessFile(filename, "rw");
            disk.setLength(totalSize);

            computeMetaDataBytes();

            fentryTable = new FEntry[MAXFILES];
            fnodesTable = new FNode[MAXBLOCKS];
            freeBlockList = new boolean[MAXBLOCKS];

            loadMetaData();
            rebuildFreeBlocks();

        } catch (IOException e) {
            throw new RuntimeException("Disk creation failed", e);
        }
    }

    // =================== METADATA CALC ===================
    private void computeMetaDataBytes() {
        fentryRegionBytes = MAXFILES * FENTRY_BYTES;
        fnodeRegionBytes = MAXBLOCKS * FNODE_BYTES;
        metadataBytes = fentryRegionBytes + fnodeRegionBytes;

        firstDataBlockIndex = (metadataBytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
    }

    private long offsetOfFEntry(int i) { return (long) i * FENTRY_BYTES; }
    private long offsetOfFNode(int j) { return (long) fentryRegionBytes + j * FNODE_BYTES; }
    private long offsetOfBlock(int blockIndex) { return (long) blockIndex * BLOCK_SIZE; }

    // =================== INTERNAL HELPERS ===================
    private int findFileInternal(String name) {
        for (int i = 0; i < MAXFILES; i++) {
            if (fentryTable[i] != null && fentryTable[i].getFilename().equals(name))
                return i;
        }
        return -1;
    }

    private int findFreeSlotInternal() {
        for (int i = 0; i < MAXFILES; i++) {
            if (fentryTable[i] == null)
                return i;
        }
        return -1;
    }

    private int findFreeNodeInternal() {
        for (int i = 0; i < MAXBLOCKS; i++) {
            if (fnodesTable[i] == null)
                return i;
        }
        return -1;
    }

    private int findFreeDataBlockInternal() {
        for (int i = firstDataBlockIndex; i < MAXBLOCKS; i++) {
            if (freeBlockList[i])
                return i;
        }
        return -1;
    }

    private int blocksNeeded(int n) {
        return (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    }

    // =================== METADATA LOAD/SAVE ===================
    private void loadMetaData() throws IOException {
        // FEntries
        for (int i = 0; i < MAXFILES; i++) {
            disk.seek(offsetOfFEntry(i));
            byte[] raw = new byte[FENTRY_BYTES];
            disk.read(raw);

            boolean empty = true;
            for (byte b : raw) if (b != 0) { empty = false; break; }
            if (empty) { fentryTable[i] = null; continue; }

            int end = 0;
            while (end < 11 && raw[end] != 0) end++;
            String name = new String(raw, 0, end);

            short size = (short)(((raw[11] & 0xFF) << 8) | (raw[12] & 0xFF));
            short first = (short)(((raw[13] & 0xFF) << 8) | (raw[14] & 0xFF));

            fentryTable[i] = new FEntry(name, size, first);
        }

        // FNodes
        for (int i = 0; i < MAXBLOCKS; i++) {
            disk.seek(offsetOfFNode(i));
            byte[] raw = new byte[FNODE_BYTES];
            disk.read(raw);

            boolean empty = true;
            for (byte b : raw) if (b != 0) { empty = false; break; }
            if (empty) { fnodesTable[i] = null; continue; }

            short block = (short)(((raw[0] & 0xFF) << 8) | (raw[1] & 0xFF));
            short next = (short)(((raw[2] & 0xFF) << 8) | (raw[3] & 0xFF));

            FNode node = new FNode(block);
            node.setNext(next);
            fnodesTable[i] = node;
        }
    }

    private void saveMetaData() throws IOException {
        // FEntries
        for (int i = 0; i < MAXFILES; i++) {
            disk.seek(offsetOfFEntry(i));
            FEntry fe = fentryTable[i];
            byte[] raw = new byte[FENTRY_BYTES];

            if (fe != null) {
                byte[] name = fe.getFilename().getBytes();
                int len = Math.min(name.length, 11);
                System.arraycopy(name, 0, raw, 0, len);

                raw[11] = (byte)(fe.getFilesize() >> 8);
                raw[12] = (byte) fe.getFilesize();
                raw[13] = (byte)(fe.getFirstBlock() >> 8);
                raw[14] = (byte) fe.getFirstBlock();
            }

            disk.write(raw);
        }

        // FNodes
        for (int i = 0; i < MAXBLOCKS; i++) {
            disk.seek(offsetOfFNode(i));
            FNode fn = fnodesTable[i];
            byte[] raw = new byte[FNODE_BYTES];

            if (fn != null) {
                raw[0] = (byte)(fn.getBlockIndex() >> 8);
                raw[1] = (byte) fn.getBlockIndex();
                raw[2] = (byte)(fn.getNext() >> 8);
                raw[3] = (byte) fn.getNext();
            }

            disk.write(raw);
        }
    }

    private void rebuildFreeBlocks() {
        Arrays.fill(freeBlockList, true);
        for (int i = 0; i < firstDataBlockIndex; i++)
            freeBlockList[i] = false;

        for (FNode fn : fnodesTable) {
            if (fn != null)
                freeBlockList[fn.getBlockIndex()] = false;
        }
    }

    // =================== CREATE ===================
    public void createFile(String name) throws Exception {
        lock.writeLock().lock();
        try {
            if (name == null || name.isEmpty()) throw new Exception("Invalid name");
            if (findFileInternal(name) != -1) throw new Exception("File exists");

            int free = findFreeSlotInternal();
            if (free == -1) throw new Exception("Max files");

            fentryTable[free] = new FEntry(name, (short)0, (short)-1);
            saveMetaData();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // =================== READ ===================
    public byte[] readFile(String name) throws Exception {
        lock.readLock().lock();
        try {
            int fileIndex = findFileInternal(name);
            if (fileIndex == -1)
                throw new Exception("ERROR: file " + name + " does not exist");

            FEntry entry = fentryTable[fileIndex];
            short fileSize = entry.getFilesize();
            short firstNode = entry.getFirstBlock();

            if (fileSize == 0 || firstNode == -1)
                return new byte[0];

            byte[] output = new byte[fileSize];
            int written = 0;
            short nodeIndex = firstNode;

            while (nodeIndex != -1 && written < fileSize) {
                FNode fn = fnodesTable[nodeIndex];
                if (fn == null) break;

                int block = fn.getBlockIndex();
                disk.seek(offsetOfBlock(block));

                int toRead = Math.min(fileSize - written, BLOCK_SIZE);
                disk.readFully(output, written, toRead);

                written += toRead;
                nodeIndex = (short) fn.getNext();
            }

            return output;
        } finally {
            lock.readLock().unlock();
        }
    }

    // =================== WRITE (APPEND) - OPTIMIZED ===================
    public void writeFile(String name, byte[] data) throws Exception {
        // First, read the current file content and metadata under read lock
        byte[] oldContent;
        FEntry entrySnapshot;
        int fileIndex;

        lock.readLock().lock();
        try {
            fileIndex = findFileInternal(name);
            if (fileIndex == -1)
                throw new Exception("ERROR: file " + name + " does not exist");

            FEntry entry = fentryTable[fileIndex];
            entrySnapshot = new FEntry(entry.getFilename(), entry.getFilesize(), entry.getFirstBlock());

            // Read current content if any exists
            if (entry.getFilesize() > 0 && entry.getFirstBlock() != -1) {
                oldContent = readFileInternal(entry);
            } else {
                oldContent = new byte[0];
            }
        } finally {
            lock.readLock().unlock();
        }

        // Combine old content with new data (outside of lock for performance)
        byte[] combined = new byte[oldContent.length + data.length];
        System.arraycopy(oldContent, 0, combined, 0, oldContent.length);
        System.arraycopy(data, 0, combined, oldContent.length, data.length);

        // Now acquire write lock for the actual modification
        lock.writeLock().lock();
        try {
            // Re-verify file still exists and get current entry
            fileIndex = findFileInternal(name);
            if (fileIndex == -1)
                throw new Exception("ERROR: file " + name + " does not exist during write");

            FEntry entry = fentryTable[fileIndex];

            // Free old blocks if they exist
            short oldNode = entry.getFirstBlock();
            while (oldNode != -1) {
                FNode fn = fnodesTable[oldNode];
                if (fn == null) break;

                freeBlockList[fn.getBlockIndex()] = true;
                int next = fn.getNext();
                fnodesTable[oldNode] = null;
                oldNode = (short) next;
            }

            if (combined.length == 0) {
                entry.setFilesize((short)0);
                entry.setFirstBlock((short)-1);
                saveMetaData();
                return;
            }

            // Allocate and write new blocks
            int requiredBlocks = blocksNeeded(combined.length);
            short head = -1;
            short prev = -1;
            int cursor = 0;

            for (int i = 0; i < requiredBlocks; i++) {
                int nodeIndex = findFreeNodeInternal();
                int blockIndex = findFreeDataBlockInternal();
                if (nodeIndex == -1 || blockIndex == -1)
                    throw new Exception("No free space");

                FNode newNode = new FNode(blockIndex);
                newNode.setNext(-1);
                fnodesTable[nodeIndex] = newNode;

                if (head == -1) head = (short) nodeIndex;
                else fnodesTable[prev].setNext(nodeIndex);

                prev = (short) nodeIndex;
                freeBlockList[blockIndex] = false;

                // Write data to block
                int chunkSize = Math.min(BLOCK_SIZE, combined.length - cursor);
                disk.seek(offsetOfBlock(blockIndex));
                disk.write(combined, cursor, chunkSize);
                if (chunkSize < BLOCK_SIZE) {
                    // Pad remaining space with zeros
                    disk.write(new byte[BLOCK_SIZE - chunkSize]);
                }
                cursor += chunkSize;
            }

            entry.setFilesize((short) combined.length);
            entry.setFirstBlock(head);
            saveMetaData();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Helper method to read file content without acquiring additional locks
    private byte[] readFileInternal(FEntry entry) throws IOException {
        short fileSize = entry.getFilesize();
        short firstNode = entry.getFirstBlock();

        if (fileSize == 0 || firstNode == -1)
            return new byte[0];

        byte[] output = new byte[fileSize];
        int written = 0;
        short nodeIndex = firstNode;

        while (nodeIndex != -1 && written < fileSize) {
            FNode fn = fnodesTable[nodeIndex];
            if (fn == null) break;

            int block = fn.getBlockIndex();
            disk.seek(offsetOfBlock(block));

            int toRead = Math.min(fileSize - written, BLOCK_SIZE);
            disk.readFully(output, written, toRead);

            written += toRead;
            nodeIndex = (short) fn.getNext();
        }

        return output;
    }

    // =================== DELETE ===================
    public void deleteFile(String name) throws Exception {
        lock.writeLock().lock();
        try {
            int fileIndex = findFileInternal(name);
            if (fileIndex == -1)
                throw new Exception("ERROR: file " + name + " does not exist");

            FEntry entry = fentryTable[fileIndex];

            short node = entry.getFirstBlock();
            while (node != -1) {
                FNode fn = fnodesTable[node];
                if (fn == null) break;

                int block = fn.getBlockIndex();

                disk.seek(offsetOfBlock(block));
                disk.write(new byte[BLOCK_SIZE]);
                freeBlockList[block] = true;

                int nxt = fn.getNext();
                fnodesTable[node] = null;
                node = (short) nxt;
            }

            fentryTable[fileIndex] = null;
            saveMetaData();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // =================== LIST ===================
    public String[] listFiles() {
        lock.readLock().lock();
        try {
            return Arrays.stream(fentryTable)
                    .filter(e -> e != null)
                    .map(FEntry::getFilename)
                    .toArray(String[]::new);
        } finally {
            lock.readLock().unlock();
        }
    }
}