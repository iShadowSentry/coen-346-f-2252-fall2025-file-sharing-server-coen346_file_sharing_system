package ca.concordia.filesystem.datastructures;

public class FNode {

    private int blockIndex;
    private int next;

    public FNode(int blockIndex) {
        this.blockIndex = blockIndex;
        this.next = -1;
    }

    public int getBlockIndex() {
        return blockIndex;
    }

    public int getNext() {
        return next;
    }

    public void setNext(int next) {
        this.next = next;
    }
}
