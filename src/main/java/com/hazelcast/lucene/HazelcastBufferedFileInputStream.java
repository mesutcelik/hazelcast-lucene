package com.hazelcast.lucene;

import org.apache.lucene.store.BufferedIndexInput;

import java.io.IOException;

public class HazelcastBufferedFileInputStream extends BufferedIndexInput {

    HazelcastFileInputStream fs;
	
	public HazelcastBufferedFileInputStream(HazelcastFileInputStream s) {
		fs = s;
	}

	@Override
	protected void readInternal(byte[] b, int off, int len)
			throws IOException {
		fs.readBytes(b, off, len);
	}

	@Override
	protected void seekInternal(long pos) throws IOException {
		fs.seek(pos);
	}

	@Override
	public void close() throws IOException {
		fs.close();
	}

	@Override
	public long length() {
		return fs.length();
	}

}
