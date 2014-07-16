package com.hazelcast.lucene;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HazelcastFileOutputStream extends IndexOutput implements Cloneable {
	
	private HazelcastFile file;
	
	public HazelcastFileOutputStream(HazelcastFile fl) throws IOException {
		file = fl;
		reset();
	}
	
	public void reset() throws IOException{
		file.seek(0);
	}

	@Override
	public void close() throws IOException {
		flush();
		file.close();
	}

	@Override
	public void flush() throws IOException {
		file.flush();
	}

	@Override
	public long getFilePointer() {
		return file.tell();
	}

	@Override
	public long length() throws IOException {
		return file.size();
	}

	@Override
	public synchronized void seek(long pos) throws IOException {	
		file.seek(pos);
	}

	@Override
	public void writeByte(byte b) throws IOException {
		file.write(ByteBuffer.allocate(1).put(b).array(), 0, 1);
	}

	@Override
	public void writeBytes(byte[] b, int offset, int len) throws IOException {
		file.write(b, offset, len);
	}

}
