package com.hazelcast.lucene;

import java.io.IOException;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class HazelcastLockFactory extends LockFactory {
	
	protected ShardedJedisPool pool;
	
	public HazelcastLockFactory(ShardedJedisPool pl) {
		pool = pl;
	}

	@Override
	public void clearLock(String name) throws IOException {
		ShardedJedis jds = pool.getResource();
		jds.del(name+".lock");
		pool.returnResource(jds);
	}

	@Override
	public Lock makeLock(String name) {
		return new HazelcastLock(name, pool);
	}

}
