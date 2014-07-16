package com.hazelcast.lucene.directory;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Created by mesutcelik on 2/14/14.
 */
public class HOutputStream  extends IndexOutput{

    static final int BUFFER_SIZE = 1024 ;

    private HFile file;
    private String name;
    private byte[] currentBuffer;
    private int currentBufferIndex;

    private int bufferPosition;
    private long bufferStart;
    private int bufferLength;


    public HOutputStream(String name,HFile file) {
        this.name=name;
        this.file = file;
        currentBufferIndex = -1;
        currentBuffer = null;
    }

    /** Copy the current contents of this buffer to the named output. */
    public void writeTo(IndexOutput out) throws IOException {
        flush();
        final long end = file.length;
        long pos = 0;
        int buffer = 0;
        while (pos < end) {
            int length = BUFFER_SIZE;
            long nextPos = pos + length;
            if (nextPos > end) {                        // at the last buffer
                length = (int)(end - pos);
            }
            out.writeBytes(file.getBuffer(buffer++), length);
            pos = nextPos;
        }
        HazelcastDirectory.fileCache.put(name,file);
    }

    /** Copy the current contents of this buffer to output
     *  byte array */
    public void writeTo(byte[] bytes, int offset) throws IOException {
        flush();
        final long end = file.length;
        long pos = 0;
        int buffer = 0;
        int bytesUpto = offset;
        while (pos < end) {
            int length = BUFFER_SIZE;
            long nextPos = pos + length;
            if (nextPos > end) {                        // at the last buffer
                length = (int)(end - pos);
            }
            System.arraycopy(file.getBuffer(buffer++), 0, bytes, bytesUpto, length);
            bytesUpto += length;
            pos = nextPos;
        }
        HazelcastDirectory.fileCache.put(name,file);
    }

    /** Resets this to an empty file. */
    public void reset() {
        currentBuffer = null;
        currentBufferIndex = -1;
        bufferPosition = 0;
        bufferStart = 0;
        bufferLength = 0;
        file.setLength(0);
        HazelcastDirectory.fileCache.put(name,file);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void seek(long pos) throws IOException {
        // set the file length in case we seek back
        // and flush() has not been called yet
        setFileLength();
        if (pos < bufferStart || pos >= bufferStart + bufferLength) {
            currentBufferIndex = (int) (pos / BUFFER_SIZE);
            switchCurrentBuffer();
        }

        bufferPosition = (int) (pos % BUFFER_SIZE);
        HazelcastDirectory.fileCache.put(name,file);
    }

    @Override
    public long length() {
        return file.length;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (bufferPosition == bufferLength) {
            currentBufferIndex++;
            switchCurrentBuffer();
        }
        currentBuffer[bufferPosition++] = b;
 //       OffHeapHazelcastDirectory.fileCache.put(name,file);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int len) throws IOException {
         assert b != null;
        while (len > 0) {
            if (bufferPosition ==  bufferLength) {
                currentBufferIndex++;
                switchCurrentBuffer();
            }

            int remainInBuffer = currentBuffer.length - bufferPosition;
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            System.arraycopy(b, offset, currentBuffer, bufferPosition, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
            bufferPosition += bytesToCopy;
        }
 //       OffHeapHazelcastDirectory.fileCache.put(name,file);
    }

    private final void switchCurrentBuffer() {
        if (currentBufferIndex == file.numBuffers()) {
            currentBuffer = file.addBuffer(BUFFER_SIZE);
        } else {
            currentBuffer = file.getBuffer(currentBufferIndex);
        }
        bufferPosition = 0;
        bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
        bufferLength = currentBuffer.length;
    }

    private void setFileLength() {
        long pointer = bufferStart + bufferPosition;
        if (pointer > file.length) {
            file.setLength(pointer);
        }
    }

    @Override
    public void flush() throws IOException {
        setFileLength();
        HazelcastDirectory.fileCache.put(name,file);
    }

    @Override
    public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
    }

    /** Returns byte usage of all buffers. */
    public long sizeInBytes() {
        return (long) file.numBuffers() * (long) BUFFER_SIZE;
    }

}