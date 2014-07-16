package com.hazelcast.lucene;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import org.apache.lucene.store.Lock;

import java.io.IOException;

public class HazelcastLock extends Lock {
	
	String name;
    HazelcastInstance instance;
    ILock lock;
	
	public HazelcastLock(String name, HazelcastInstance instance) {
		this.name = name;
		this.instance = instance;
        lock =instance.getLock(name);
	}

	@Override
	public boolean isLocked() throws IOException {
        return lock.isLocked();
	}

	@Override
	public boolean obtain() throws IOException {
		if( isLocked() )
			return false;

        lock.lock();
        return true;
	}

	@Override
	public void release() throws IOException {
        lock.unlock();
	}

}
