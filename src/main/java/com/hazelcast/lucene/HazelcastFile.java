package com.hazelcast.lucene;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HazelcastFile {
	
	protected HazelcastDirectory directory;
    private IMap<byte[],byte[]> fileMap;

	private String name;
	protected long fileLength;
	protected long currPos, currBlock;
	
	private byte[] nameBytes;
	private byte[] pathBytes;
	
	protected int BufferLength;
	protected boolean dirtyBuffer;
	protected byte[] buffer;
	
	protected boolean fileExtended;

	public HazelcastFile(String name, HazelcastDirectory dir,HazelcastInstance instance) throws IOException {
		this.directory = dir;
		this.fileMap = instance.getMap(name);
		this.name = name;
		this.currBlock = this.currPos = 0;
		
		this.BufferLength = HazelcastDirectory.FILE_BUFFER_SIZE;
		this.buffer = null;
		this.dirtyBuffer = false;
		this.fileExtended = false;
		
		size();
		readBuffer();
	}
	
	public synchronized long size() {
        byte [] p = directory.getDirectoryMap().get(getName());

		if( p != null && p.length == Long.SIZE/8 ){
			this.fileLength = ByteBuffer.wrap(p).asLongBuffer().get();
		}
		return this.fileLength;
	}
	
	public synchronized void flush() throws IOException {
		flushBuffer();
	}
	
	protected synchronized void flushBuffer() throws IOException {
		if( dirtyBuffer ) {
			if( HazelcastDirectory.COMPRESSED ){
				byte[] compressed = Snappy.compress(buffer);
                fileMap.set(blockAddress(),compressed);
			}else{
                fileMap.set(blockAddress(), buffer);
			}
			if( fileExtended ){
                directory.getDirectoryMap().set(getName(),ByteBuffer.allocate(Long.SIZE/8).putLong(fileLength).array());
				directory.reloadSizeFromFiles();
				fileExtended = false;
			}
		}
		dirtyBuffer = false;
	}
	
	protected synchronized void readBuffer() throws IOException {
		buffer = fileMap.get(blockAddress());
		if( buffer != null && HazelcastDirectory.COMPRESSED) {
			buffer = Snappy.uncompress(buffer);
		}
		if( buffer == null || buffer.length != BufferLength ){
			buffer = new byte [this.BufferLength];
		}
	}
	
	private byte[] blockAddress() {
		ByteBuffer buff = ByteBuffer.allocate(getPathBytes().length+(Long.SIZE/8));
		buff.put(getPathBytes()).putLong(currBlock);
		return buff.array();
	}
	
	public synchronized void close() throws IOException {
		flush();
	}
	
	public String getName() {
		return name;
	}
	
	public byte[] getNameBytes() {
		if( nameBytes == null ) {
			nameBytes = name.getBytes();
		}
		return nameBytes;
	}
	
	public long blocksRequired(long size) {
		return blockPos(size) + ((size % BufferLength == 0) ? 0 : 1);
	}
	
	public long blockPos(long i) {
		return (i / BufferLength);
	}
	
	public synchronized void seek(long p) throws IOException {
		// If seek remains within current block
		if( blockPos(p) == currBlock ){
			currPos = p;
			if( fileLength < currPos ) {
				fileLength = currPos;
				fileExtended = true;
			}
			return;
		}
		//Seeking somewhere within existing blocks
		flushBuffer();
		currPos = p;
		currBlock = blockPos(p);
		if( fileLength < currPos ) {
			fileLength = currPos;
			fileExtended = true;
		}
		readBuffer();
	}
	
	public long tell() {
		return currPos;
	}
		
	public synchronized void read(byte[] buff, int offset, long n) throws IOException {
		int sourceBufferIndex = (int)(currPos % BufferLength);
		int bytesRead = BufferLength - sourceBufferIndex;
		int destBufferIndex = offset;
		long bytesLeft = n;
		
		if( bytesRead > bytesLeft ) bytesRead = (int) bytesLeft;
		while( bytesLeft > 0 ){
			//Read and move on!
			System.arraycopy(buffer, sourceBufferIndex, buff, destBufferIndex, bytesRead);
			seek(currPos + bytesRead);

			bytesLeft -= bytesRead;
			destBufferIndex += bytesRead;
			sourceBufferIndex = 0;

			bytesRead = BufferLength;
			if( bytesRead > bytesLeft ) bytesRead = (int) bytesLeft;
		}
	}
	
	public synchronized void write(byte[] buff, int offset, long n) throws IOException{
		int sourceBufferIndex = (int)(currPos % BufferLength);
		int bytesWrite = BufferLength - sourceBufferIndex;
		int destBufferIndex = offset;
		long bytesLeft = n;
		
		if( bytesWrite > bytesLeft ) bytesWrite = (int) bytesLeft;
		while( bytesLeft > 0 ){
			//Read and move on!
			System.arraycopy(buff, destBufferIndex, buffer, sourceBufferIndex, bytesWrite);
			dirtyBuffer = true;
			seek(currPos + bytesWrite);


			bytesLeft -= bytesWrite;
			destBufferIndex += bytesWrite;
			sourceBufferIndex = 0;

			bytesWrite = BufferLength;
			if( bytesWrite > bytesLeft ) bytesWrite = (int) bytesLeft;
		}		
	}
	
	public synchronized void delete() {
        directory.getDirectoryMap().delete(getName());
        fileMap.destroy();
		dirtyBuffer = false;
	}

	public String getPath() {
		String parent = "";
		if( directory != null ) parent = directory.getDirName();
		return String.format("@%s:%s", parent, name);
	}
	
	public byte[] getPathBytes() {
		if( pathBytes == null )
			pathBytes = getPath().getBytes();
		return pathBytes;
	}

}
