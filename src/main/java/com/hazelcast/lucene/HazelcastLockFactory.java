package com.hazelcast.lucene;

import com.hazelcast.core.HazelcastInstance;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;

public class HazelcastLockFactory extends LockFactory {
	
	protected HazelcastInstance instance;
	
	public HazelcastLockFactory(HazelcastInstance instance) {
		this.instance = instance;
	}

	@Override
	public void clearLock(String name) throws IOException {
        instance.getLock(name).destroy();
	}

	@Override
	public Lock makeLock(String name) {
		return new HazelcastLock(name, instance);
	}

}
