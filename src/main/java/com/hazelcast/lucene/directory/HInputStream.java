package com.hazelcast.lucene.directory;

import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * Created by mesutcelik on 2/14/14.
 */
public class HInputStream extends IndexInput implements Cloneable {
    static final int BUFFER_SIZE = HOutputStream.BUFFER_SIZE;

    private HFile file;
    private long length;


    private byte[] currentBuffer;
    private int currentBufferIndex;

    private int bufferPosition;
    private long bufferStart;
    private int bufferLength;

    public HInputStream(HFile file,String name) throws IOException {
        super("HInputStream(name=" + name + ")");
        this.file=file;
        length = file.length;
        if (length/BUFFER_SIZE >= Integer.MAX_VALUE) {
            throw new IOException("RAMInputStream too large length=" + length + ": " + name);
        }

        // make sure that we switch to the
        // first needed buffer lazily
        currentBufferIndex = -1;
        currentBuffer = null;
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (bufferPosition >= bufferLength) {
            currentBufferIndex++;
            switchCurrentBuffer(true);
        }
        return currentBuffer[bufferPosition++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (bufferPosition >= bufferLength) {
                currentBufferIndex++;
                switchCurrentBuffer(true);
            }

            int remainInBuffer = bufferLength - bufferPosition;
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            System.arraycopy(currentBuffer, bufferPosition, b, offset, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
            bufferPosition += bytesToCopy;
        }
    }

    private final void switchCurrentBuffer(boolean enforceEOF) throws IOException {
        bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
//        System.out.println("name:"+name+ "index:"+currentBufferIndex);
        if (currentBufferIndex >= file.numBuffers()) {
            // end of file reached, no more buffers left
            if (enforceEOF) {
                throw new EOFException("read past EOF: " + this);
            } else {
                // Force EOF if a read takes place at this position
                currentBufferIndex--;
                bufferPosition = BUFFER_SIZE;
            }
        } else {
            currentBuffer = file.getBuffer(currentBufferIndex);
            bufferPosition = 0;
            long buflen = length - bufferStart;
            bufferLength = buflen > BUFFER_SIZE ? BUFFER_SIZE : (int) buflen;
        }
    }

    @Override
    public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (currentBuffer==null || pos < bufferStart || pos >= bufferStart + BUFFER_SIZE) {
            currentBufferIndex = (int) (pos / BUFFER_SIZE);
            switchCurrentBuffer(false);
        }
        bufferPosition = (int) (pos % BUFFER_SIZE);
    }


}
