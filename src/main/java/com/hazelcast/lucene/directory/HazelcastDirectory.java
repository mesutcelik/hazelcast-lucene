package com.hazelcast.lucene.directory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mesutcelik on 2/11/14.
 */
public class HazelcastDirectory extends BaseDirectory {

    protected static List<String> fileNames = new ArrayList<String>();
    protected static IMap<String, HFile> fileCache;
    protected final AtomicLong sizeInBytes = new AtomicLong();

    public HazelcastDirectory() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        fileCache = instance.getMap("hazelcastDirectory");
        try {
            // TODO : develop Hazelcast Lock Factory
            setLockFactory(new SingleInstanceLockFactory());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return fileNames.toArray(new String[fileNames.size()]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        ensureOpen();
        return fileNames.contains(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        HFile file = fileCache.get(name);
        fileNames.remove(name);
        sizeInBytes.addAndGet(-file.sizeInBytes);

    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        HFile file = fileCache.get(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }
        return file.getLength();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
         HFile file = new HFile();
        HFile existing = fileCache.get(name);
        if (existing != null) {
            sizeInBytes.addAndGet(-existing.sizeInBytes);
            //    existing.directory = null;
        }
        fileCache.put(name, file);
        fileNames.add(name);
        return new HOutputStream(name,file);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        HFile file = fileCache.get(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }
        return new HInputStream(file,name);
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        fileCache.clear();

    }

    public void destroy() {
        fileCache.destroy();
    }

}
