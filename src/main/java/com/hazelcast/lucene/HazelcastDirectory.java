package com.hazelcast.lucene;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class HazelcastDirectory extends Directory implements Serializable {
	public  static int FILE_BUFFER_SIZE = 256 * 1024;
	public	static boolean COMPRESSED = false;
    private static String SIZE = "size";
	
	private static final long serialVersionUID = 7378532726794782140L;
	private HazelcastInstance instance;
	private String dirName;
	private byte[] dirNameBytes;
	
	private long directorySize;


    protected IMap<String,byte[]> directoryMap;
	
	public HazelcastDirectory(String name, HazelcastInstance instance) {
		this.instance = instance;
		dirName = name;
        directoryMap = instance.getMap(dirName);
        open();
		try {
			setLockFactory(new HazelcastLockFactory(instance));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void setLockFactory(LockFactory lockFactory) throws IOException {
		if(lockFactory instanceof HazelcastLockFactory)
			super.setLockFactory(lockFactory);
	}
	
	private void open() {
        byte[] size = directoryMap.get(SIZE);
		directorySize = 0;
		try {
			directorySize = ByteBuffer.wrap(size).asLongBuffer().get();
		}catch(Exception e){
			reloadSizeFromFiles();
		}
	}
	
	public boolean exists() {

        return directoryMap != null;
	}
	
	public void reloadSizeFromFiles() {
		try { directorySize = dirSize(); } catch (IOException e) {}
	}
	
	private long dirSize() throws IOException {
		if( directoryMap == null || directoryMap.isEmpty() )
			return 0;

        long ret = 0;
        for(byte[] sz: directoryMap.values() ){
			try{ ret += ByteBuffer.wrap(sz).asLongBuffer().get(); }catch(Exception e){}
		}

		return ret;
	}

	@Override
	public synchronized void close() throws IOException {
		directorySize = dirSize();
        directoryMap.set(SIZE, ByteBuffer.allocate(Long.SIZE / 8).putLong(directorySize).array());

	}

	@Override
	public IndexOutput createOutput(String filename) throws IOException {
		return new HazelcastFileOutputStream( new HazelcastFile(filename, this, instance) );
	}

	@Override
	public void deleteFile(String filename) throws IOException {
		new HazelcastFile(filename, this, instance).delete();
	}

	@Override
	public boolean fileExists(String filename) throws IOException {
        return directoryMap.containsKey(filename);
	}

	@Override
	public long fileLength(String filename) throws IOException {
		return new HazelcastFile(filename, this, instance).size();
	}

    @Override
    public String[] listAll() throws IOException {
        if (directoryMap == null || directoryMap.isEmpty()) {
            return new String[0];
        }
        int size = directoryMap.keySet().size();
        return directoryMap.keySet().toArray(new String[size]);
    }

    @Override
	public IndexInput openInput(String filename) throws IOException {
		if( !fileExists(filename) )
			throw new IOException();
		return new HazelcastBufferedFileInputStream(new HazelcastFileInputStream( filename, new HazelcastFile(filename, this, instance) ));
	}

	@Override
	@Deprecated
	public long fileModified(String filename) throws IOException {
		return 0;
	}

	@Override
	@Deprecated
	public void touchFile(String fiename) throws IOException {
		
	}

	public HazelcastInstance getInstance() {
		return instance;
	}
	
	public String getDirName() {
		return dirName;
	}
	
	public byte[] getDirNameBytes() {
		if( dirNameBytes == null )
			dirNameBytes = dirName.getBytes();
		return dirNameBytes;
	}

    public IMap<String, byte[]> getDirectoryMap() {
        return directoryMap;
    }

    public void setDirectoryMap(IMap<String, byte[]> directoryMap) {
        this.directoryMap = directoryMap;
    }

}
