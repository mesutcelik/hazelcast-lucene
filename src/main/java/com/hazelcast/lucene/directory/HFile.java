package com.hazelcast.lucene.directory;

import java.io.Serializable;
import java.util.ArrayList;

public class HFile implements Serializable {

    protected ArrayList<byte[]> buffers = new ArrayList<byte[]>();
    long length;
    protected long sizeInBytes;

    // File used as buffer, in no RAMDirectory
    public HFile() {}


    // For non-stream access from thread that might be concurrent with writing
    public synchronized long getLength() {
        return length;
    }

    protected synchronized void setLength(long length) {
        this.length = length;
    }

    protected final byte[] addBuffer(int size) {
        byte[] buffer = newBuffer(size);
        synchronized(this) {
            buffers.add(buffer);
            sizeInBytes += size;
        }
        return buffer;
    }

    protected final synchronized byte[] getBuffer(int index) {
        return buffers.get(index);
    }

    protected final synchronized int numBuffers() {
        return buffers.size();
    }

    /**
     * Expert: allocate a new buffer.
     * Subclasses can allocate differently.
     * @param size size of allocated buffer.
     * @return allocated buffer.
     */
    protected byte[] newBuffer(int size) {
        return new byte[size];
    }

    public synchronized long getSizeInBytes() {
        return sizeInBytes;
    }

//    @Override
//    public void writeData(ObjectDataOutput out) throws IOException {
//        out.writeLong(length);
//        out.writeLong(sizeInBytes);
//        out.writeObject(buffers);
//
//    }
//
//    @Override
//    public void readData(ObjectDataInput in) throws IOException {
//        length=in.readLong();
//        sizeInBytes=in.readLong();
//        buffers=in.readObject();
//    }
}
